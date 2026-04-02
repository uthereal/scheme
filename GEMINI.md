# Scheme CLI - Code Generator Architecture & Standards

## Overview
This document outlines the architectural decisions, design patterns, and rigorous performance optimizations established during the development of the `scheme` framework's schema parsing and code generation.

Our primary goal is to build an ultra-fast, strictly-typed query builder that operates at bare-metal `database/sql` speeds without relying on the `reflect` package or behaving like a traditional, heavyweight ORM.

---

## 1. Core Architectural Decisions

### 1.1. Rigid Query Builder vs. Full ORM
The generated code acts as a **rigid query builder**, not an ORM.
*   **Explicit Mapping:** The builder assumes single-table queries. The framework completely avoids complex alias tracking (`t.id`) and underlying tuple hydration nightmares.
*   **Strict Struct Mapping:** Every query builder (`UserQuery`, `PostQuery`) is hard-tied to its respective generated model struct (`User`, `Post`).
*   **Collision-Safe Naming:** The parser scans across all tables, enums, domains, and composite types in the database globally. If a naming collision occurs across different schemas (e.g. `public.users` vs `auth.users`), the generator automatically prefixes the struct name with the schema (e.g. `PublicUser` vs `AuthUser`).

### 1.2. Zero Reflection Scanning (Closure Mappers)
The biggest performance bottleneck in typical Go ORMs is using `reflect` to map database columns to struct fields at runtime.
*   **The Solution:** We extract the column-to-struct-pointer resolution *outside* the `rows.Next()` loop using strongly-typed closures.
*   **Alias Safety:** Columns natively support `As("alias")`. The internal mapping logic explicitly evaluates and strips SQL alias identifiers (`strings.Index(col, " AS ")`) to ensure the underlying generic switch statements never fail to bind memory pointers.

### 1.3. Eager Loading (The Dataloader Pattern)
Rather than executing massive, Cartesian-exploding `LEFT JOIN` operations to hydrate nested graphs, the `loadEdges()` function uses an in-memory `O(N)` hash mapping technique.
*   **O(1) Queries:** If 100 Users are fetched, it runs exactly 1 additional query (`SELECT * FROM posts WHERE user_id = ANY(ARRAY[...])`) and stitches the structs directly into the parent nodes.
*   **Infinite Recursion:** Because `loadEdges` internally invokes `.Get()` on the child QueryBuilder, deeply nested eager loads (e.g. `Users -> Posts -> Images`) correctly and infinitely recurse without creating N+1 issues.

### 1.4. Immutability & Safety
*   **Immutable Builder State:** To prevent cross-pollution when a base query is shared across multiple goroutines, the builder is strictly immutable. Every chainable modifier performs a shallow copy of the query struct.
*   **Connection Leak Prevention:** Methods returning data strictly `defer rows.Close()`, while fire-and-forget mutations utilize `ExecContext` to instantly release database/sql pool connections.

---

## 2. Formatting & Code Standards

### 2.1. The 80-Character Limit
*   **Strict Width:** Every `.go` file and `.go.tmpl` template file is strictly formatted to a maximum of **80 characters per line**. This rule applies to `query.WriteString(...)` logic, long `fmt.Errorf` chains, and anonymous lambda functions.

### 2.2. Variable Declarations
*   **No Inline `if` Variables:** Inline variable assignments inside `if` blocks (e.g., `if err := doThing(); err != nil {`) are explicitly banned. Variables must be hoisted above the `if` block for enhanced readability and debugging.
    ```go
    // BAD:
    if idx := strings.LastIndexByte(colName, '.'); idx >= 0 { ... }
    
    // GOOD:
    idx := strings.LastIndexByte(colName, '.')
    if idx >= 0 { ... }
    ```

### 2.3. Punctuation & Syntax
*   **`slog` Logging:** Logger messages must be complete sentences. They must begin with a capital letter and end with a period `.` (e.g., `logger.Error("The schema name is invalid.")`).
*   **Error Wrapping:** Native Go errors must NOT be complete sentences. They must start with a lowercase letter, must not contain ending punctuation, and must use the `->` delimiter for wrapping (e.g., `fmt.Errorf("failed to map base type -> %w", err)`).
*   **Slice Determinism:** When transforming internal memory maps (Hash Maps) into template-ready slices, the slices must be explicitly sorted (e.g. `sort.Slice(...)`) to ensure generated files remain 100% deterministic byte-for-byte across builds.

---

## 3. Protobuf Architecture

### 3.1. The 1-1-1 Rule
*   Every `.proto` file strictly adheres to the **1-1-1 Rule**: 1 File = 1 primary `message` or `enum` definition.
*   **Exception (`data_type.proto`):** The only explicit exception to this rule is `spec/proto/postgres/data_type.proto`, which acts as a consolidated wrapper for 50+ underlying database primitives to prevent massive file fragmentation.

### 3.2. Go Package Prefixing
*   Cross-schema Go cyclic imports are prevented by grouping all schemas within a specific engine instance into a single, unified Go package (e.g. `package mydb`).
*   To support absolute import mapping, the CLI utilizes `-go-pkg-prefix` (e.g. `github.com/org/repo/gen`).
*   This config is natively passed down into the `gen.Language` object via the `Options.GoPackagePath` property, completely decoupling the underlying template execution from CLI flag parsing parameters.

---

## 5. Testing Standards

### 5.1. Table-Driven & Asserts
*   **Table-Driven:** Use table-driven tests (`[]struct{...}`) for all unit and logic testing to guarantee massive edge-case coverage without duplicated boilerplate.
*   **Assertions:** Assertions must use the `got, want := ..., ...` assignment and validation flow logic format on a single line where possible.

### 5.2. Organization & Scoping
*   **White Box (Internal):** Use `package mypkg` in `{{file}}_test.go` to test unexported logic (e.g. lowercase closure mappers).
*   **Black Box (External):** Use `package mypkg_test` in `{{file}}_test.go` to test the public API as a consumer, verifying cross-package integrity and preventing import cycles.
*   **Setup/Teardown:** Use `main_test.go` (in the same package) to contain shared `TestMain(m *testing.M)` setup logic, keeping individual `{{file}}_test.go` files highly focused.

---

## 6. API Surface Area
The following methods terminate the builder chain and execute against the `DBQuerier`:
*   `Get(ctx)`: Returns `[]*Model`.
*   `First(ctx)`: Returns `*Model` (Wrapped in `LIMIT 1`, returns `nil, nil` if not found).
*   `Exists(ctx)`: Returns `bool` (Uses optimized `SELECT EXISTS(...)`).
*   `Paginate(ctx, page, pageSize)`: Returns `*ModelPagination`. Uses highly efficient `COUNT(*) OVER()` window functions.
*   `Create`, `CreateMany`, `Update`, `Upsert`, `UpsertMany`, `Delete`.