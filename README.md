# change_master
mysql slave's master host change to another middle master or the high level master

Introduction:

scena1:
    Original topology
        A ---> B
          +--> C
    make C replicate from B(we call it 'down'), after change it look like this
        A ---> B ---> C
scena2:
    Original topology
        A ---> B ---> C
    make C replicate from A(we call it 'up'), after change it look like this
        A ---> B
          +--> C
scena3:
    Original topology
        A ---> B ---> C
                  +--> D
    make C replicate from D(we call it 'middle'), after change it look like this
        A ---> B
          +--> D ---> C

Depedency:
    golang 1.3+
    go get code.google.com/p/gcfg
    go get github.com/go-sql-driver/mysql

Installation:
    go build -o change_master main.go

Features:

Usage:
