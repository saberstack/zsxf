(ns org.zsxf.archive.compiler.todo)


;TODO in order
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
; 1. Transform :where to directed acyclic graph (DAG)

;  - transduce: trans-form each nested relation, reduce into a DAG

; 2. Transform DAG to code similar to example in org.zsxf.experimental.dataflow/incremental-computation-xf
;  - either macro or anonymous fn calls
; 3. PoC query demo: datalog->dag->code
