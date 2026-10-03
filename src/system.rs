use std::net::SocketAddr;

/// Nameservers from the OS resolver config (read once by the caller), port 53, de-duplicated, order kept.
pub fn system_nameservers() -> Result<Vec<SocketAddr>, String> {
    let (cfg, _opts) =
        hickory_resolver::system_conf::read_system_conf().map_err(|e| e.to_string())?;
    let mut out: Vec<SocketAddr> = Vec::new();
    for ns in cfg.name_servers() {
        let a = SocketAddr::new(ns.ip, 53);
        if !out.contains(&a) {
            out.push(a);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_nameservers_does_not_panic() {
        // Containers may lack resolv.conf; only assert the call is well-behaved.
        let _ = system_nameservers();
    }
}
