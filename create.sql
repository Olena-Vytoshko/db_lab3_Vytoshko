CREATE TYPE Status_Game_Type AS ENUM ('draw', 'resign', 'mate', 'outoftime');
CREATE TYPE Winner_Type AS ENUM ('white', 'black', 'draw');

CREATE TABLE IF NOT EXISTS public.white_player
(
    white_player_id character varying(40) NOT NULL,
    white_player_registration_date date NOT NULL,
    white_player_rating integer NOT NULL,
    CONSTRAINT white_id PRIMARY KEY (white_player_id)
);

CREATE TABLE IF NOT EXISTS public.black_player
(
    black_player_id character varying(40) NOT NULL,
    black_player_registration_date date NOT NULL,
    black_player_rating integer NOT NULL,
    CONSTRAINT black_id PRIMARY KEY (black_player_id)
);

CREATE TABLE IF NOT EXISTS public.games
(
    game_id character varying(40) NOT NULL,
    white_player_id character varying(40) NOT NULL,
    black_player_id character varying(40) NOT NULL,
    rated boolean NOT NULL,
    start_time timestamp without time zone NOT NULL,
    end_time timestamp without time zone NOT NULL,
    number_of_turns integer NOT NULL,
    game_status status_game_type NOT NULL,
    winner winner_type,
    CONSTRAINT game_id PRIMARY KEY (game_id),
    CONSTRAINT fwhite_id FOREIGN KEY (white_player_id)
        REFERENCES public.white_player (white_player_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID,
    CONSTRAINT fblack_id FOREIGN KEY (black_player_id)
        REFERENCES public.black_player (black_player_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
        NOT VALID
);

