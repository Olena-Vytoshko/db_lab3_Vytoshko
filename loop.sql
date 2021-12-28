DO
$do$
BEGIN 
   FOR i IN 1..25 LOOP
      INSERT INTO public.white_player
      VALUES(
	  	md5(random()::text),
		timestamp '2014-01-10 20:00:00' + random() * (timestamp '2014-01-20 20:00:00' - timestamp '2014-01-10 10:00:00'),
		random() * 1200  + 800
	  );
   END LOOP;
END
$do$;