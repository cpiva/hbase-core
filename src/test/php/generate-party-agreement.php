<?php
require_once '/home/kaufman/projects/Faker/src/autoload.php';
$faker = Faker\Factory::create();
for ($i=0; $i < 100; $i++) {

    $lastname = $faker->lastName;
    $name = addslashes($faker->firstName . " " . substr($lastname,0,1));

    $rowkey = date($format = 'Ymd')."-0000-".str_pad($i+1,10,'0',STR_PAD_LEFT);

    echo $rowkey."|"."04-1020-".str_pad($i+1,8,'0',STR_PAD_LEFT)."|IND|999\n";

}
