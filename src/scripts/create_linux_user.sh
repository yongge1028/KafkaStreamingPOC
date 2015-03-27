#!/bin/bash
groupadd faganpe
useradd -g faganpe -m -s /bin/bash faganpe
su faganpe -c "mkdir -p /home/faganpe/spark-addons/MaxMind"
