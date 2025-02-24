[sql]
select v1 from (select v1,v2,v3 from t0 except select v4,v5,v6 from t1) as a
[result]
EXCEPT
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    SCAN (columns[7: v4, 8: v5, 9: v6] predicate[null])
[end]

[sql]
select v1 from (select v1,v2,v3 from t0 intersect select v4,v5,v6 from t1) as a
[result]
INTERSECT
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    SCAN (columns[7: v4, 8: v5, 9: v6] predicate[null])
[end]

[sql]
select v1 from (select v1,v2,v3 from t0 union all select v4,v5,v6 from t1) as a
[result]
UNION
    SCAN (columns[1: v1] predicate[null])
    SCAN (columns[7: v4] predicate[null])
[end]

[sql]
select v1,v2,v3 from t0 union select v4,v5,v6 from t1
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[4: v1, 5: v2, 6: v3]] having [null]
    EXCHANGE SHUFFLE[4, 5, 6]
        AGGREGATE ([LOCAL] aggregate [{}] group by [[4: v1, 5: v2, 6: v3]] having [null]
            UNION
                SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
                SCAN (columns[7: v4, 8: v5, 9: v6] predicate[null])
[end]

[sql]
select v1,v2,v3 from t0 union all select v4,v5,v6 from t1
[result]
UNION
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[null])
    SCAN (columns[7: v4, 8: v5, 9: v6] predicate[null])
[end]

[sql]
select v1 from t0 union all select v4 from t1 union all select v7 from t2;
[result]
UNION
    SCAN (columns[1: v1] predicate[null])
    SCAN (columns[5: v4] predicate[null])
    SCAN (columns[8: v7] predicate[null])
[end]

[sql]
select v1 from t0 except select v4 from t1 except select v7 from t2;
[result]
EXCEPT
    SCAN (columns[1: v1] predicate[null])
    SCAN (columns[5: v4] predicate[null])
    SCAN (columns[8: v7] predicate[null])
[end]

[sql]
select v1 from t0 intersect select v4 from t1 intersect select v7 from t2;
[result]
INTERSECT
    SCAN (columns[1: v1] predicate[null])
    SCAN (columns[5: v4] predicate[null])
    SCAN (columns[8: v7] predicate[null])
[end]

[sql]
with testC (v) as (select v1 from t0 union all select v4 from t1 union all select v7 from t2) select * from testC;
[result]
UNION
    SCAN (columns[1: v1] predicate[null])
    SCAN (columns[5: v4] predicate[null])
    SCAN (columns[8: v7] predicate[null])
[end]

[sql]
select v1 from t0 intersect select v4 from t1 union select v7 from t2;
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[8: v1]] having [null]
    EXCHANGE SHUFFLE[8]
        AGGREGATE ([LOCAL] aggregate [{}] group by [[8: v1]] having [null]
            UNION
                INTERSECT
                    SCAN (columns[1: v1] predicate[null])
                    SCAN (columns[5: v4] predicate[null])
                SCAN (columns[9: v7] predicate[null])
[end]

[sql]
select v1 from t0 except select v4 from t1 union select v7 from t2;
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[8: v1]] having [null]
    EXCHANGE SHUFFLE[8]
        AGGREGATE ([LOCAL] aggregate [{}] group by [[8: v1]] having [null]
            UNION
                EXCEPT
                    SCAN (columns[1: v1] predicate[null])
                    SCAN (columns[5: v4] predicate[null])
                SCAN (columns[9: v7] predicate[null])
[end]

[sql]
select v1 from t0 except select v4 from t1 union all select v7 from t2;
[result]
UNION
    EXCEPT
        SCAN (columns[1: v1] predicate[null])
        SCAN (columns[5: v4] predicate[null])
    SCAN (columns[9: v7] predicate[null])
[end]

[sql]
select v1 from (select v1 from t0 union all select v4 from t1 union all select v7 from t2) a
[result]
UNION
    SCAN (columns[1: v1] predicate[null])
    SCAN (columns[5: v4] predicate[null])
    SCAN (columns[8: v7] predicate[null])
[end]

[sql]
select v1,sum(v2) from t0 group by v1 union all select v4,v5 from t1;
[result]
UNION
    AGGREGATE ([GLOBAL] aggregate [{4: sum(2: v2)=sum(4: sum(2: v2))}] group by [[1: v1]] having [null]
        AGGREGATE ([LOCAL] aggregate [{4: sum(2: v2)=sum(2: v2)}] group by [[1: v1]] having [null]
            SCAN (columns[1: v1, 2: v2] predicate[null])
    SCAN (columns[7: v4, 8: v5] predicate[null])
[end]

[sql]
select v1,sum(v2) from t0 group by v1 union select v4,v5 from t1;
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[5: v1, 6: sum(2: v2)]] having [null]
    EXCHANGE SHUFFLE[5, 6]
        AGGREGATE ([LOCAL] aggregate [{}] group by [[5: v1, 6: sum(2: v2)]] having [null]
            UNION
                AGGREGATE ([GLOBAL] aggregate [{4: sum(2: v2)=sum(4: sum(2: v2))}] group by [[1: v1]] having [null]
                    AGGREGATE ([LOCAL] aggregate [{4: sum(2: v2)=sum(2: v2)}] group by [[1: v1]] having [null]
                        SCAN (columns[1: v1, 2: v2] predicate[null])
                SCAN (columns[7: v4, 8: v5] predicate[null])
[end]

[sql]
select a,b from (select v1 as a,sum(v2) as b from t0 group by v1 union all select v4,v5 from t1) t group by a,b;
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[5: v1, 6: sum(2: v2)]] having [null]
    EXCHANGE SHUFFLE[5, 6]
        AGGREGATE ([LOCAL] aggregate [{}] group by [[5: v1, 6: sum(2: v2)]] having [null]
            UNION
                AGGREGATE ([GLOBAL] aggregate [{4: sum(2: v2)=sum(4: sum(2: v2))}] group by [[1: v1]] having [null]
                    AGGREGATE ([LOCAL] aggregate [{4: sum(2: v2)=sum(2: v2)}] group by [[1: v1]] having [null]
                        SCAN (columns[1: v1, 2: v2] predicate[null])
                SCAN (columns[7: v4, 8: v5] predicate[null])
[end]

[sql]
select v from (select sum(v1) as v from t0 union select v4 from t1 except select v1 /2 + 0.5 from t0) a;
[result]
EXCEPT
    AGGREGATE ([GLOBAL] aggregate [{}] group by [[6: cast]] having [null]
        EXCHANGE SHUFFLE[6]
            AGGREGATE ([LOCAL] aggregate [{}] group by [[6: cast]] having [null]
                UNION
                    AGGREGATE ([GLOBAL] aggregate [{4: sum(1: v1)=sum(4: sum(1: v1))}] group by [[]] having [null]
                        EXCHANGE GATHER
                            AGGREGATE ([LOCAL] aggregate [{4: sum(1: v1)=sum(1: v1)}] group by [[]] having [null]
                                SCAN (columns[1: v1] predicate[null])
                    SCAN (columns[7: v4] predicate[null])
    SCAN (columns[12: v1] predicate[null])
[end]

[sql]
select v from (select v1 as v from t0 union all select v4 from t1) a where v = 1
[result]
UNION
    SCAN (columns[1: v1] predicate[1: v1 = 1])
    SCAN (columns[5: v4] predicate[5: v4 = 1])
[end]

[sql]
select v from (select v1 as v from t0 union select v4 from t1) a where v = 1
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[4: v1]] having [null]
    EXCHANGE SHUFFLE[4]
        AGGREGATE ([LOCAL] aggregate [{}] group by [[4: v1]] having [null]
            UNION
                SCAN (columns[1: v1] predicate[1: v1 = 1])
                SCAN (columns[5: v4] predicate[5: v4 = 1])
[end]

[sql]
select v from (select sum(v1) as v from t0 union select v4 from t1) a where v = 1
[result]
AGGREGATE ([GLOBAL] aggregate [{}] group by [[5: sum(1: v1)]] having [null]
    EXCHANGE SHUFFLE[5]
        AGGREGATE ([LOCAL] aggregate [{}] group by [[5: sum(1: v1)]] having [null]
            UNION
                AGGREGATE ([GLOBAL] aggregate [{4: sum(1: v1)=sum(4: sum(1: v1))}] group by [[]] having [4: sum(1: v1) = 1]
                    EXCHANGE GATHER
                        AGGREGATE ([LOCAL] aggregate [{4: sum(1: v1)=sum(1: v1)}] group by [[]] having [null]
                            SCAN (columns[1: v1] predicate[null])
                SCAN (columns[6: v4] predicate[6: v4 = 1])
[end]

[sql]
select v from (select v1 as v from t0 except select v4 from t1) a where v = 1
[result]
EXCEPT
    SCAN (columns[1: v1] predicate[1: v1 = 1])
    SCAN (columns[5: v4] predicate[5: v4 = 1])
[end]

[sql]
select v from (select v1 as v from t0 intersect select v4 from t1) a where v = 1
[result]
INTERSECT
    SCAN (columns[1: v1] predicate[1: v1 = 1])
    SCAN (columns[5: v4] predicate[5: v4 = 1])
[end]

[sql]
select * from (select v2,v3,v1 from t0 union all select v4,v5,v6 from t1) a where a.v3 = 3
[result]
UNION
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[3: v3 = 3])
    SCAN (columns[7: v4, 8: v5, 9: v6] predicate[8: v5 = 3])
[end]

[sql]
select v1 from (select v1, v2 from t0 union all select 1,1 from t1) t
[result]
UNION
    SCAN (columns[1: v1] predicate[null])
    SCAN (columns[6: v4] predicate[null])
[end]

[sql]
select v1 from (select 1 as v1, v2 from t0 union all select 1,1 from t1) t
[result]
UNION
    SCAN (columns[1: v1] predicate[null])
    SCAN (columns[7: v4] predicate[null])
[end]