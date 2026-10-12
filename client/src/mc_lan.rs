//! Minecraft 局域网（LAN）广播监听。
//!
//! 背景：MC 在「对局域网开放」后会**随机分配**一个端口（不一定是 25565），
//! 并把端口写进每隔约 1.5 秒发一次的 UDP 广播里：
//!
//! ```text
//! [MOTD]世界名[/MOTD][AD]端口[/AD]
//! ```
//!
//! 广播发往 IPv4 多播地址 `224.0.2.60:4445` / IPv6 多播地址 `ff75:230::60:4445`
//! （见 Minecraft Wiki「本地局域网联机」，以及 `LanServerPinger` 的实现）。
//!
//! 内网穿透场景下，用户往往在配置里把 `local_port` 写成 25565 —— 但真正在跑的
//! MC 世界可能开在别的端口上，于是穿透过去连的是个空端口。这里做的是：
//! **当代理的本地端口被写成 25565 时，先监听广播，收到谁就把流量转给谁**；
//! 一段时间内没收到任何广播，再回落到配置里写的 25565（即"就当它没开放局域网"）。
//!
//! 监听是**进程级**的常驻任务：只在启动时绑一次 socket，跨会话重连持续有效。

use std::{
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{
        atomic::{AtomicU16, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::net::UdpSocket;
use tracing::{debug, info, warn};

/// Minecraft 局域网发现协议使用的 IPv4 多播地址。
const MC_MULTICAST_V4: Ipv4Addr = Ipv4Addr::new(224, 0, 2, 60);
/// Minecraft 局域网发现协议使用的 IPv6 多播地址。
const MC_MULTICAST_V6: Ipv6Addr = Ipv6Addr::new(0xff75, 0x230, 0, 0, 0, 0, 0, 0x60);
/// MC 局域网广播收发端口。
const MC_MULTICAST_PORT: u16 = 4445;

/// 一条广播记录「多久算过期」。
///
/// MC 每 1.5 秒广播一次，这里给 10 倍的宽容度：世界卡顿时广播会稀疏，
/// 窗口太短会把仍然活着的世界误判成"没开"。
const BROADCAST_TTL: Duration = Duration::from_secs(15);

/// 共享的「最近一次发现的端口」。
///
/// 用 `AtomicU16` 而不是 `Mutex<Option<u16>>`：读侧在**每条工作连接**上都要读一次
/// （见 `main.rs` 的 `effective_local_addr`），锁竞争毫无必要。
/// `0` 表示"当前没有有效的广播端口"。
#[derive(Debug, Default)]
pub struct LanPort {
    port: AtomicU16,
    /// 端口最后一次被更新的 Unix 时间戳（秒）。`0` 表示从未收到过。
    updated_at: AtomicU64,
}

impl LanPort {
    /// 取当前有效的端口：没有广播、或广播已过期时返回 `None`。
    pub fn current(&self) -> Option<u16> {
        let port = self.port.load(Ordering::Relaxed);
        if port == 0 {
            return None;
        }
        let at = self.updated_at.load(Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if now.saturating_sub(at) > BROADCAST_TTL.as_secs() {
            return None;
        }
        Some(port)
    }

    /// 记下一次广播里解析出来的端口。
    fn record(&self, port: u16) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.port.store(port, Ordering::Relaxed);
        self.updated_at.store(now, Ordering::Relaxed);
    }
}

/// 从一条广播载荷里解出端口。
///
/// 载荷形如 `"[MOTD]Steve 的世界[/MOTD][AD]54321[/AD]"`。MOTD 可有可无
/// （只有 `[AD]` 是必需的），解析失败返回 `None`。
///
/// 这里手写扫描而**不引入正则**：一是少一个依赖，二是 MC 的广播格式极其稳定
/// （写死的两个标记），正则反而是杀鸡用牛刀。
fn parse_broadcast_port(payload: &str) -> Option<u16> {
    const OPEN: &str = "[AD]";
    const CLOSE: &str = "[/AD]";
    let start = payload.find(OPEN)? + OPEN.len();
    let rest = &payload[start..];
    let end = rest.find(CLOSE)?;
    rest[..end].trim().parse::<u16>().ok().filter(|p| *p != 0)
}

/// 启动局域网广播监听（进程级，常驻）。
///
/// 返回共享的端口表；调用方把它交给工作连接读取。监听任务本身**不返回错误** ——
/// 绑不上 socket（比如被占用）时只打一条 warn，让穿透本身照常按 25565 跑，
/// 不能因为"监听广播失败"就把整个客户端拖垮。
pub fn spawn_listener() -> Arc<LanPort> {
    let shared = Arc::new(LanPort::default());

    // IPv4：绑定多播端口并加入多播组。
    {
        let shared = shared.clone();
        tokio::spawn(async move {
            let bind = SocketAddr::from((Ipv4Addr::UNSPECIFIED, MC_MULTICAST_PORT));
            let socket = match bind_multicast_v4(bind).await {
                Ok(s) => s,
                Err(e) => {
                    warn!("监听 Minecraft 局域网广播（IPv4）失败，将回落到配置里的本地端口：{e}");
                    return;
                }
            };
            info!(
                "已开始监听 Minecraft 局域网广播：{}:{}（IPv4）",
                MC_MULTICAST_V4, MC_MULTICAST_PORT
            );
            recv_loop(socket, shared).await;
        });
    }

    // IPv6：独立 socket（双栈模式下 v4 组可能收不到 v6 多播，分开更稳）。
    {
        let shared = shared.clone();
        tokio::spawn(async move {
            let bind = SocketAddr::from((Ipv6Addr::UNSPECIFIED, MC_MULTICAST_PORT));
            let socket = match bind_multicast_v6(bind).await {
                Ok(s) => s,
                Err(e) => {
                    debug!("监听 Minecraft 局域网广播（IPv6）失败，忽略：{e}");
                    return;
                }
            };
            recv_loop(socket, shared).await;
        });
    }

    shared
}

async fn bind_multicast_v4(bind: SocketAddr) -> std::io::Result<UdpSocket> {
    // 用 socket2 风格的原始 socket 配置，但避免引入新依赖：tokio 的
    // `UdpSocket::bind` 之后用 `std` 的 `getsockopt`/`setsockopt` 不可用，
    // 因此这里直接用 `socket2` 是不行的 —— 改走标准库能表达的部分：
    // `join_multicast_v4` 由 tokio 提供，`SO_REUSEADDR` 由下面的 `setsockopt`
    // 通过 socket 原生句柄设置。
    let socket = UdpSocket::bind(bind).await?;
    set_reuse_addr(&socket)?;
    socket.join_multicast_v4(MC_MULTICAST_V4, Ipv4Addr::UNSPECIFIED)?;
    Ok(socket)
}

async fn bind_multicast_v6(bind: SocketAddr) -> std::io::Result<UdpSocket> {
    let socket = UdpSocket::bind(bind).await?;
    socket.join_multicast_v6(&MC_MULTICAST_V6, 0)?;
    Ok(socket)
}

/// 在 tokio 的 UDP socket 上设置 `SO_REUSEADDR`。
///
/// 为什么需要：同一个局域网里可能同时跑着好几个客户端（或 MC 本体自己的监听），
/// 不设 `SO_REUSEADDR` 会有"地址已在使用"的绑定失败。在 Windows / Linux 上
/// `SO_REUSEADDR` 都允许对**多播**地址的重复绑定（这不是 TCP 那种端口复用，
/// 多播场景下是标准做法）。
#[cfg(unix)]
fn set_reuse_addr(socket: &UdpSocket) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    let fd = socket.as_raw_fd();
    let one: libc::c_int = 1;
    // SAFETY: fd 来自存活的 socket；`one` 是合法的 int 缓冲区。
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEADDR,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(windows)]
fn set_reuse_addr(socket: &UdpSocket) -> std::io::Result<()> {
    // Windows 上 SO_REUSEADDR 语义不同（会劫持端口），多播监听不需要它。
    // 绑定失败时由调用方打 warn 并回落，不做额外处理。
    let _ = socket;
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn set_reuse_addr(socket: &UdpSocket) -> std::io::Result<()> {
    let _ = socket;
    Ok(())
}

/// 收包 → 解析 → 记录端口。单条报文出错只跳过，不影响后续。
async fn recv_loop(socket: UdpSocket, shared: Arc<LanPort>) {
    let mut buf = [0u8; 1024];
    loop {
        match socket.recv_from(&mut buf).await {
            Ok((n, from)) => {
                // MC 的广播载荷是 UTF-8；非法字节直接跳过这条报文。
                let Ok(text) = std::str::from_utf8(&buf[..n]) else {
                    continue;
                };
                if let Some(port) = parse_broadcast_port(text) {
                    debug!(%from, port, "发现 Minecraft 局域网世界");
                    shared.record(port);
                }
            }
            Err(e) => {
                // 单次收包失败（如被信号打断）不该终止整个监听。
                debug!("读取 Minecraft 广播失败，继续监听：{e}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

/// MC 局域网「对局域网开放」时默认端口的约定值。
pub const MC_DEFAULT_PORT: u16 = 25565;

/// 从 `"host:port"` 形式的本地地址里取出端口。
///
/// 取不出来（没有端口、端口非法）时返回 `None`。IPv6 字面量形如
/// `"[::1]:25565"`，这里用「最后一个冒号」切分即可正确处理两种写法。
pub fn local_port_of(local_addr: &str) -> Option<u16> {
    let idx = local_addr.rfind(':')?;
    local_addr[idx + 1..].trim().parse::<u16>().ok()
}

/// 按「只有本地端口是 25565 时才用广播端口」的规则，算出**这次真正要连**的地址。
///
/// 规则：
/// - 本地端口不是 25565（用户显式指定了别的端口）→ **原样返回**，不碰广播；
/// - 是 25565 且广播里有端口 → 用广播端口**替换掉主机名后的端口**；
/// - 是 25565 但没收到广播（或已过期）→ 回落到 25565（原样返回）。
///
/// 保留主机名不变：用户可能写 `localhost:25565` 甚至 `192.168.1.5:25565`，
/// 广播端口是同一台机器上 MC 世界开的端口，主机名沿用配置里的即可。
pub fn effective_local_addr(local_addr: &str, lan: &LanPort) -> String {
    let Some(port) = local_port_of(local_addr) else {
        return local_addr.to_string();
    };
    if port != MC_DEFAULT_PORT {
        return local_addr.to_string();
    }
    let Some(discovered) = lan.current() else {
        return local_addr.to_string();
    };
    let Some(idx) = local_addr.rfind(':') else {
        return local_addr.to_string();
    };
    let host = &local_addr[..idx];
    format!("{host}:{discovered}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 解析标准广播载荷() {
        assert_eq!(
            parse_broadcast_port("[MOTD]Steve 的世界[/MOTD][AD]54321[/AD]"),
            Some(54321)
        );
    }

    #[test]
    fn 只有ad也能解析() {
        assert_eq!(parse_broadcast_port("[AD]25565[/AD]"), Some(25565));
    }

    #[test]
    fn 缺ad或ad非法一律返回none() {
        assert_eq!(parse_broadcast_port("[MOTD]只有世界名[/MOTD]"), None);
        assert_eq!(parse_broadcast_port("[AD][/AD]"), None);
        assert_eq!(parse_broadcast_port("[AD]abc[/AD]"), None);
        assert_eq!(parse_broadcast_port("[AD]0[/AD]"), None, "0 端口视为无效");
        assert_eq!(parse_broadcast_port("[AD]70000[/AD]"), None, "超出 u16");
    }

    #[test]
    fn 从本地地址取端口() {
        assert_eq!(local_port_of("127.0.0.1:25565"), Some(25565));
        assert_eq!(local_port_of("localhost:8080"), Some(8080));
        assert_eq!(local_port_of("[::1]:25565"), Some(25565));
        assert_eq!(local_port_of("没有端口"), None);
    }

    #[test]
    fn 端口不是25565时不动广播() {
        let lan = LanPort::default();
        lan.record(54321);
        // 用户显式写了别的端口 → 广播再新鲜也不能改
        assert_eq!(
            effective_local_addr("127.0.0.1:8080", &lan),
            "127.0.0.1:8080"
        );
    }

    #[test]
    fn 端口是25565且有广播时用广播端口() {
        let lan = LanPort::default();
        lan.record(54321);
        assert_eq!(
            effective_local_addr("127.0.0.1:25565", &lan),
            "127.0.0.1:54321"
        );
        // 主机名 / 内网 IP 都保留，只替换端口
        assert_eq!(
            effective_local_addr("192.168.1.5:25565", &lan),
            "192.168.1.5:54321"
        );
    }

    #[test]
    fn 端口是25565但没广播时回落到25565() {
        let lan = LanPort::default();
        assert_eq!(
            effective_local_addr("127.0.0.1:25565", &lan),
            "127.0.0.1:25565"
        );
    }

    #[test]
    fn 广播过期后回落() {
        let lan = LanPort::default();
        lan.record(54321);
        // 手动把更新时间戳往前推，模拟超过 TTL
        lan.updated_at
            .store(0, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(lan.current(), None);
        assert_eq!(
            effective_local_addr("127.0.0.1:25565", &lan),
            "127.0.0.1:25565"
        );
    }

    /// ★ 需求原文是「本地端口在 **.ini** 中定义为 25565」。
    ///
    /// 配置层会把 INI 的 `local_ip` / `local_port` 两段式合并成
    /// `local_addr = "127.0.0.1:25565"`（见 `common/src/frp_legacy.rs` 的
    /// `樱花下发的_ini_能被识别并解析`）。这里把那条链路与本节功能接上：
    /// 真解析一份 `.ini`，确认合并出来的地址恰好会被本模块判定为「要跟随广播」。
    #[test]
    fn ini里写local_port为25565的代理能被识别为需要跟随广播() {
        let ini = r#"
[common]
server_addr = frp.example.com
server_port = 7000

[MC]
type = tcp
local_ip = 127.0.0.1
local_port = 25565
remote_port = 25565
"#;
        let cfg = nfrp_common::config::parse_client(ini).expect("ini 应能解析");
        assert_eq!(cfg.proxies.len(), 1);
        let proxy = &cfg.proxies[0];
        assert_eq!(proxy.local_addr, "127.0.0.1:25565");
        // 正是这个端口让工作连接去查广播
        assert_eq!(local_port_of(&proxy.local_addr), Some(MC_DEFAULT_PORT));

        let lan = LanPort::default();
        lan.record(51234);
        assert_eq!(
            effective_local_addr(&proxy.local_addr, &lan),
            "127.0.0.1:51234"
        );
    }
}
