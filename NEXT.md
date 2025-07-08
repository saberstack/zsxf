## Next

(notes on WIP features)
###### parameterized queries

Parameterized queries can be viewed as a pre-compilation step where we transform a query's `:in` variables into values that `zsxf` manages. For example:

```clojure
;a regular query a developer writes  
'[:find ?person-name  
  :in $ ?something  
  :where  
  [?p :person/name ?person-name]  
  [?p :person/likes ?something]]  
  
;after zsxf "pre-compilation" step  
'[:find ?person-name  
  :where  
  [?p :person/name ?person-name]  
  [?p :person/likes ?something]  
  [_ :zsxf/tmp-param-1 ?something]]
```

###### as-of simple edition

- save every root of the query state when data changes, done
    - it will use a more memory if your data changes a ton
    - less if append only
- random as-of after the fact?
    - more tricky
    - not sure if possible in a more efficient way than the above
