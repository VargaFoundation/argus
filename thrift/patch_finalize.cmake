# Post-process thrift c_glib generated code to fix a GObject memory leak.
#
# The thrift compiler's c_glib generator emits a `finalize` for every struct
# that overrides GObjectClass->finalize but never chains to the parent
# finalizer. GObject's own finalize (which clears each instance's qdata) is
# therefore never run, leaking a small allocation per object — and thrift
# result sets create many objects per query. Inject the missing parent chain.
#
# The injection anchor (the exact per-finalize comment + THRIFT_UNUSED_VAR line)
# is unique to finalize functions, so a literal replace is safe and portable
# (no regex, no external tools). Idempotent: skips a file already patched.
#
# Usage: cmake -DFILE=<generated.c> -P patch_finalize.cmake

if(NOT EXISTS "${FILE}")
    return()
endif()

file(READ "${FILE}" _content)

set(_marker "/* argus: chain to parent finalize */")
string(FIND "${_content}" "${_marker}" _already)
if(NOT _already EQUAL -1)
    return()  # already patched
endif()

set(_anchor "  /* satisfy -Wall in case we don't use tobject */\n  THRIFT_UNUSED_VAR (tobject);\n")
set(_replacement "${_anchor}  ${_marker}\n  G_OBJECT_CLASS (g_type_class_peek (g_type_parent (G_OBJECT_TYPE (object))))->finalize (object);\n")

string(REPLACE "${_anchor}" "${_replacement}" _content "${_content}")
file(WRITE "${FILE}" "${_content}")
