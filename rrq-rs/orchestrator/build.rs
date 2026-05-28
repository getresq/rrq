use std::fmt::Write as _;
use std::path::{Path, PathBuf};

const LUA_SCRIPTS: &[&str] = &[
    "src/lua/lock_and_start.lua",
    "src/lua/claim_ready.lua",
    "src/lua/refresh_lock.lua",
    "src/lua/release_lock_if_owner.lua",
    "src/lua/retry.lua",
    "src/lua/enqueue.lua",
    "src/lua/move_to_dlq.lua",
    "src/lua/requeue_job.lua",
];

fn main() {
    let manifest_dir = match std::env::var_os("CARGO_MANIFEST_DIR") {
        Some(value) => PathBuf::from(value),
        None => panic!("CARGO_MANIFEST_DIR is not set for build script"),
    };

    for relative_path in LUA_SCRIPTS {
        let script_path = manifest_dir.join(relative_path);
        println!("cargo:rerun-if-changed={}", script_path.display());
        validate_lua_script(&script_path);
    }
}

fn validate_lua_script(script_path: &Path) {
    let source = std::fs::read_to_string(script_path).unwrap_or_else(|error| {
        panic!(
            "failed to read Lua script '{}': {error}",
            script_path.display()
        )
    });

    let parse_result =
        full_moon::parse_fallible(&source, full_moon::LuaVersion::lua51()).into_result();
    if let Err(errors) = parse_result {
        let mut details = String::new();
        for error in errors {
            let (start, _) = error.range();
            let _ = writeln!(
                details,
                "line {}, col {}: {}",
                start.line(),
                start.character(),
                error.error_message()
            );
        }
        panic!(
            "invalid Lua syntax in '{}':\n{details}",
            script_path.display()
        );
    }
}
