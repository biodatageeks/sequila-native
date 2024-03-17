fn main() {
    cxx_build::bridge("src/main.rs")
        .flag_if_supported("-std=c++17")
        .opt_level(3)
        .compile("cxxbridge-sequila");

    println!("cargo:rerun-if-changed=src/main.rs");
    println!("cargo:rerun-if-changed=include/IITree.h");
    println!("cargo:rerun-if-changed=include/IITreeBFS.h");
    println!("cargo:rerun-if-changed=include/iitii.h");
}
