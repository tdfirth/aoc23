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

illegal_games as (
    select id
    from games_by_id
    where
        (color = 'red' and n > 12)
        or (color = 'green' and n > 13)
        or (color = 'blue' and n > 14)
)

select sum(distinct id) from games_by_id where id not in (select id from illegal_games);
