# run all steps with prompts

clear
read -p "0 drop ?"
./step0.sh
read -p "1 restore ?"
./step1.sh
read -p "2 metadata ?"
./step2.sh
read -p "3 raw ?"
./step3.sh
read -p "4 dw ?"
./step4.sh
read -p "3+ delta raw ?"
./step3plus.sh
read -p "4+ delta dw & merge ?"
./step4plus.sh

exit
