#!/bin/bash

echo '************************************************'
echo '** Setting config parameters                  **'
echo '**                                            **'
echo '************************************************'

        #key in file to chane      location of config file
sed -i "/input_file/s/= .*/= $1/" ./config.ini

sed -i "/tdl/s/= .*/= $2/" ./config.ini

sed -i "/tdu/s/= .*/= $3/" ./config.ini


echo '************************************************'
echo '**          Config parameters set             **'
echo '************************************************'
