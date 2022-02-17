-- 13. Write a SQL query to find all the defenders who scored a goal for their teams.


select  distinct pm.player_name
from goal_details gd, 
	 player_mast pm,
     playing_position pp ,  
     soccer_country sc 
where gd.player_id = pm.player_id 
  and pm.posi_to_play = pp.position_id
  and pm.team_id = sc.country_id
  and position_id in ('DF', 'FD') 
  ;

-----------------  solution----------------------------

+------------------------+---------------------+
| player_name            | country_name        |
+------------------------+---------------------+
| Olivier Giroud         | France              |
| Bogdan Stancu          | Romania             |
| Fabian Schar           | Switzerland         |
| Gareth Bale            | Wales               |
| Hal Robson-Kanu        | Wales               |
| Vasili Berezutski      | Russia              |
| Arkadiusz Milik        | Poland              |
| Thomas Muller          | Germany             |
| Gerard Pique           | Spain               |
| Ciaran Clark           | Republic of Ireland |
| Graziano Pelle         | Italy               |
| Adam Szalai            | Hungary             |
| Nani                   | Portugal            |
| Admir Mehmedi          | Switzerland         |
| Antoine Griezmann      | France              |
| Jamie Vardy            | England             |
| Daniel Sturridge       | England             |
| Gareth McAuley         | Northern Ireland    |
| Niall McGinn           | Northern Ireland    |
| Eder                   | Italy               |
| Milan Skoda            | Czech Republic      |
| TomasNecid             | Czech Republic      |
| Alvaro Morata          | Spain               |
| Nolito                 | Spain               |
| Romelu Lukaku          | Belgium             |
| Birkir Saevarsson      | Iceland             |
| Armando Sadiku         | Albania             |
| Neil Taylor            | Wales               |
| Mario Gomez            | Germany             |
| Burak Yilmaz           | Turkey              |
| Nikola Kalinic         | Croatia             |
| Jon Dadi Bodvarsson    | Iceland             |
| Zoltan Gera            | Hungary             |
| Balazs Dzsudzsak       | Hungary             |
| Cristiano Ronaldo      | Portugal            |
| Ricardo Quaresma       | Portugal            |
| Jerome Boateng         | Germany             |
| Toby Alderweireld      | Belgium             |
| Michy Batshuayi        | Belgium             |
| Giorgio Chiellini      | Italy               |
| Wayne Rooney           | England             |
| Arnor Ingvi Traustason | Iceland             |
| Kolbeinn Sigthorsson   | Iceland             |
| Robert Lewandowski     | Poland              |
| Ashley Williams        | Wales               |
| Sam Vokes              | Wales               |
| Leonardo Bonucci       | Italy               |
| Eder                   | Portugal            |
+------------------------+---------------------+
48 rows in set (0.01 sec)
