make clean; make -j8

csh -f qutest 1 > q1
csh -f qutest 5 > q5
csh -f qutest 7 > q7

nvim -d q1 reference-outputs/qu1.txt
nvim -d q5 reference-outputs/qu5.txt
nvim -d q7 reference-outputs/qu7.txt
