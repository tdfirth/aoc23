with values as (
    select * from read_csv("./1/input.csv", columns = { value: text })
),

trimmed as (
    select trim(value, 'abcdefghijklmnopqrstuvwxyz') as value from values
),

first_and_last as (
    select
        left(value, 1) as first,
        right(value, 1) as last
    from trimmed
)

select sum((first || last)::int) as result
from first_and_last;
