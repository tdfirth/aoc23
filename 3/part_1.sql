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
    join  line_length l on true
    group by number_group 
),

adjacent_cells as (
    select *
    from numbers n 
    inner join line_length l on true
    inner join labelled_grid g on 
        (id between (n.start_id - 1 - l.length) and (n.end_id + 1 - l.length))
        or (id between (n.start_id - 1) and (n.end_id + 1))
        or (id between (n.start_id - 1 + l.length) and (n.end_id + 1 + l.length))
),

has_adjacent_symbols as (
    select 
        n,
        bool_or(symbol) as adjacent_symbol
    from adjacent_cells ac
    group by ac.n, ac.start_id
    order by ac.start_id
)

select sum(n) from has_adjacent_symbols where adjacent_symbol;
