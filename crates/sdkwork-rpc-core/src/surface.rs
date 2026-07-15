//! RPC surface vocabulary per `RPC_SPEC.md` and `RPC_FRAMEWORK_SPEC.md`.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RpcSurface {
    App,
    Backend,
    Internal,
    Common,
}

impl RpcSurface {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::App => "app",
            Self::Backend => "backend",
            Self::Internal => "internal",
            Self::Common => "common",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.trim() {
            "app" => Some(Self::App),
            "backend" => Some(Self::Backend),
            "internal" => Some(Self::Internal),
            "common" => Some(Self::Common),
            _ => None,
        }
    }
}

pub fn validate_rpc_surface(value: &str) -> Result<RpcSurface, String> {
    RpcSurface::parse(value).ok_or_else(|| {
        format!("rpc_surface must be one of app, backend, internal, common (got {value})")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_canonical_surfaces() {
        assert_eq!(
            validate_rpc_surface("internal").unwrap(),
            RpcSurface::Internal
        );
        assert!(validate_rpc_surface("public").is_err());
    }
}
