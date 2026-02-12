# Using koinos-state-db from a Different Branch

The koinos-cmake Hunter config uses **`external/state_db`** when it exists in the project. If that directory is present, Hunter will use it instead of the default tarball (v1.1.2). You can point it at any branch by adding the repo as a submodule and checking out the branch.

## Option 1: Git submodule (recommended)

1. **Add koinos-state-db as a submodule** (from the repo root):

   ```bash
   mkdir -p external
   git submodule add https://github.com/koinos/koinos-state-db-cpp.git external/state_db
   ```

2. **Check out the branch you want**:

   ```bash
   cd external/state_db
   git fetch origin
   git checkout <branch-name>   # e.g. fix-delta-removals, develop, etc.
   cd ../..
   ```

3. **Initialize/update submodules** (if you clone the repo elsewhere):

   ```bash
   git submodule update --init --recursive
   ```

4. **Configure and build** koinos-chain as usual. Hunter will see `external/state_db` and use it via `GIT_SUBMODULE "external/state_db"` (see koinos-cmake’s `Hunter/config.cmake`).

To **switch to another branch** later:

```bash
cd external/state_db
git fetch origin
git checkout <other-branch>
cd ../..
# Reconfigure and rebuild
```

---

## Option 2: Local Hunter config override (branch as tarball)

If you prefer not to use a submodule, you can override the package with a branch tarball. This requires making Hunter use your local config **before** koinos-cmake’s Hunter setup runs.

1. **Create a local Hunter config** that overrides `koinos_state_db`:

   Create `cmake/Hunter/config.cmake` (or another path you will reference) with:

   ```cmake
   # Override koinos_state_db to use a branch (replace BRANCH_NAME with your branch)
   hunter_config(koinos_state_db
     URL "https://github.com/koinos/koinos-state-db-cpp/archive/refs/heads/BRANCH_NAME.tar.gz"
     SHA1 ""   # Leave empty on first run; CMake will print the expected SHA1
     CMAKE_ARGS
     BUILD_TESTING=OFF
     BUILD_EXAMPLES=OFF
   )
   ```

2. **Tell Hunter to use this config** by setting `HUNTER_CONFIG_FILE` **before** the first `include()` that pulls in koinos-cmake. In your top-level `CMakeLists.txt`, add near the top (before `FetchContent_Declare(koinos_cmake ...)`):

   ```cmake
   set(HUNTER_CONFIG_FILE "${CMAKE_CURRENT_SOURCE_DIR}/cmake/Hunter/config.cmake" CACHE FILEPATH "Local Hunter overrides")
   ```

3. Run CMake; if SHA1 is empty, Hunter may complain and print the expected SHA1. Set that SHA1 in your `hunter_config(koinos_state_db ...)` and reconfigure.

Note: koinos-cmake’s HunterGate is called from inside koinos-cmake, so the exact merge behavior of `HUNTER_CONFIG_FILE` depends on how koinos-cmake includes configs. If the override is not applied, use **Option 1** (submodule) instead.

---

## Summary

- **Option 1 (submodule)**: Add `external/state_db` as a submodule, checkout your branch, and build. This matches koinos-cmake’s existing logic and is the most reliable.
- **Option 2**: Override `koinos_state_db` in a local Hunter config with a branch tarball URL; use only if you cannot or do not want to use a submodule.
