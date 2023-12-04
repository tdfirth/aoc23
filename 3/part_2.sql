with input as (
    select
        row_number() over () as row,
        line
    from read_csv("./3/input.csv", columns = { 'line': 'string' })
),

grid as (
    select
        row,
        unnest(string_split(line, '')) as cell
    from input
),

labelled_grid as (
    select
        row_number() over (order by row) as id,
        row,
        row_number() over (partition by row) as col,
        cell,
        regexp_matches(cell, '\d') as digit,
        regexp_matches(cell, '\.') as dot,
        regexp_matches(cell, '\*') as star,
        not(digit) and not(dot) as symbol
    from grid
    order by row, col
),

line_length as (
    select max(col) as length from labelled_grid
),

number_boundaries as (
    select
        row_number() over () as n,
        row, col, cell,
        case when lag(col, 1, true) over () = col - 1 then 1 else 0 end as boundary,
    from labelled_grid where digit
),

number_groups as (
    select
        *,
        case when boundary = 0 then 0 else lag(boundary) over () + boundary end as run,
        n - run as number_group
    from number_boundaries
),

numbers as (
    select 
        min(row) as n_row, 
        min(col) as n_start,
        max(col) as n_end,
        min(l.length) as rl,
        n_start + ((n_row - 1) * rl) as start_id,
        start_id + n_end - n_start as end_id,
        string_agg(cell, '')::int as n 
    from number_groups 
    join line_length l on true
    group by number_group 
),

gear_bounding_boxes as (
    select 
        id as star_id,
        id - l.length - 1 as tl,
        id - l.length + 1 as tr,
        id - 1 as ml,
        id + 1 as mr,
        id + l.length - 1 as bl,
        id + l.length + 1 as br
    from labelled_grid lg
    join line_length l on true
    where star
),

gear_adjencies as (
    select star_id, n
    from gear_bounding_boxes g
    inner join numbers n on
        n.start_id between tl and tr 
        or n.end_id between tl and tr 
        or n.start_id between ml and mr 
        or n.end_id between ml and mr 
        or n.start_id between bl and br 
        or n.end_id between bl and br
),

gears as (
    select 
        star_id,
        product(n) as gear_ratio,
        count(n) as count
    from gear_adjencies 
    group by star_id
)

select sum(gear_ratio)::int from gears where count = 2;
