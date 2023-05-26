# Collaborator Target Pipeline 

This tool will take in input file of collaborator targets in CSV
format and output daily targets.

## Build Instructions

Run `make` in the **src** directory to build an executable. 

## Source input format

The CSV input file should have the following format

```
2020;name@example.com;10000
```

So year;email;target amount

## Output format

The CSV output will have the following format

```
2020-01-01;name@example.com;27.3972603
2020-01-02;name@example.com;27.3972603
```
