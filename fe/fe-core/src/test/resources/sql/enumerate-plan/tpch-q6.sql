[sql]
select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
        l_shipdate >= date '1995-01-01'
  and l_shipdate < date '1996-01-01'
  and l_discount between 0.02 and 0.04
  and l_quantity < 24
[plan-1]
AGGREGATE ([GLOBAL] aggregate [{19: sum(18: expr)=sum(18: expr)}] group by [[]] having [null]
    EXCHANGE GATHER
        SCAN (columns[5: L_QUANTITY, 6: L_EXTENDEDPRICE, 7: L_DISCOUNT, 11: L_SHIPDATE] predicate[11: L_SHIPDATE >= 1995-01-01 AND 11: L_SHIPDATE < 1996-01-01 AND 7: L_DISCOUNT >= 0.02 AND 7: L_DISCOUNT <= 0.04 AND 5: L_QUANTITY < 24.0])
[end]
[plan-2]
AGGREGATE ([GLOBAL] aggregate [{19: sum(18: expr)=sum(19: sum(18: expr))}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{19: sum(18: expr)=sum(18: expr)}] group by [[]] having [null]
            SCAN (columns[5: L_QUANTITY, 6: L_EXTENDEDPRICE, 7: L_DISCOUNT, 11: L_SHIPDATE] predicate[11: L_SHIPDATE >= 1995-01-01 AND 11: L_SHIPDATE < 1996-01-01 AND 7: L_DISCOUNT >= 0.02 AND 7: L_DISCOUNT <= 0.04 AND 5: L_QUANTITY < 24.0])
[end]