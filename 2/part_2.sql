with input as (
    select
        split_part(split_part(game, ':', 1), ' ', 2)::int as id,
        string_split(
            replace(split_part(game, ': ', 2), ';', ','), ', '
        ) as games
    from read_csv("./2/input.csv", columns = { game: text }, delim = '')
),

unnested_games as (
    select
        id,
        unnest(games) game
    from input
),

games_by_id as (
    select
        id,
        split_part(game, ' ', 1)::int as n,
        split_part(game, ' ', 2) as color
    from unnested_games
),

min_cubes as (
    select
        id,
        color,
        max(n) as n
    from games_by_id group by id, color
),

products as (
    select
        id,
        product(n) as power
    from min_cubes group by id
)
select sum(power)::int from products;
