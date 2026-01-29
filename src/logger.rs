use env_logger::Env;

pub fn init_logger() {
    let env = if cfg!(debug_assertions) {
        Env::default().default_filter_or("debug")
    } else {
        Env::default().default_filter_or("info")
    };

    env_logger::Builder::from_env(env).init();
}
