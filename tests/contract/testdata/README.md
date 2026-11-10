# testdata

Specs shared by the core tests and, through the contract archive, by the SDKs.

- `valid/` — every file must validate without errors.
- `invalid/schema/` — each file breaks the JSON Schema; any client that vendors `schemas/` must reject it.
- `invalid/rules/` — each file is schema-valid but breaks a semantic rule of `manufacture validate`.

Each invalid file lists the expected diagnostics in comments:

```
# want: <field path> | <message substring>
```
