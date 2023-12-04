with values as (
    select * from read_csv("./1/input.csv", columns = { value: text })
),

padded as (
    select
        value as input,
        replace(value, 'nineight', 'nineeight') as nineeight,
        replace(nineeight, 'fiveight', 'fiveeight') as fiveeight,
        replace(fiveeight, 'threeight', 'threeeight') as threeeight,
        replace(threeeight, 'oneight', 'oneeight') as oneeight,
        replace(oneeight, 'eighthree', 'eightthree') as eightthree,
        replace(eightthree, 'eightwo', 'eighttwo') as eighttwo,
        replace(eighttwo, 'sevenine', 'sevennine') as sevennine,
        replace(sevennine, 'twone', 'twoone') as value
    from values
),

replaced as (
    select
        input as original,
        value as input,
        replace(value, 'nine', '9') as nine,
        replace(nine, 'eight', '8') as eight,
        replace(eight, 'seven', '7') as seven,
        replace(seven, 'six', '6') as six,
        replace(six, 'five', '5') as five,
        replace(five, 'four', '4') as four,
        replace(four, 'three', '3') as three,
        replace(three, 'two', '2') as two,
        replace(two, 'one', '1') as value
    from padded
),

trimmed as (
    select
        original,
        value as input,
        trim(value, 'abcdefghijklmnopqrstuvwxyz') as value
    from replaced
),

first_and_last as (
    select
        original,
        value as input,
        left(value, 1) as first,
        right(value, 1) as last
    from trimmed
)

select sum((first || last)::int) as result
from first_and_last;
