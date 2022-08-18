#!/bin/bash

# Get the MSDS data from S#
aws s3 cp s3://prod-mtsu-msds-data-lake-source/log-sheet-files/PME\ Use\ Digital\ Record\ OC2A\ Summer\ 2020.xlsx log-sheet/
aws s3 cp s3://prod-mtsu-msds-data-lake-source/sensor-files/OC_2A\ PME\ minidot\ 296600\ USGS\ DO\ 4\ 9_25_2020.TXT sensor/

# get solinst
aws s3 cp s3://prod-mtsu-msds-data-lake-source/log-sheet-files/Solinst\ Use\ Digital\ Record\ USGSLL6\ Ponds\ Behind\ Erie\ W2020\ CL04.xlsx log-sheet/
aws s3 cp s3://prod-mtsu-msds-data-lake-source/sensor-files/CL04_1079977_usgslevellogger6_2021_06_09_200549Compensated.csv sensor/

# get ysi
aws s3 cp s3://prod-mtsu-msds-data-lake-source/log-sheet-files/YSI\ Use\ Digital\ Record\ EPP02\ Summer\ 2020.xlsx log-sheet/
aws s3 cp s3://prod-mtsu-msds-data-lake-source/sensor-files/EPP2_832020.xlsx sensor/