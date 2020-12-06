# Control 3

### Nombre estudiante: Matías Duhalde

*Nota para ayudante: Acá se explica lo mismo, y se colocan los mismos fragmentos de código que en el Jupyter Notebook, por lo que no es necesario revisarlo muy detalladamente si ya se revisó el notebook. El README se incluyó por formalidad y para cumplir con lo del enunciado.*


## 1. Manejo de Datos

Para importar los datos desde el archivo `Resultados_plebiscito.csv` a la base de datos en SQLite3, se usó la CLI directamente. Los comandos utilizados fueron los siguientes:
```SQL
.open municipios.db

CREATE TABLE Resultados_Plebiscito(
    cod_com INTEGER PRIMARY KEY,
    apruebo INTEGER,
    rechazo INTEGER,
    blancos INTEGER,
    nulos INTEGER,
    FOREIGN KEY(cod_com) REFERENCES Comuna(id)
);

.separator ,

.import Resultados_Pleb.csv Resultados_Plebiscito
```

De esta manera, se agregan los datos del csv a una relación llamada `Resultados_Plebiscito`, con el esquema anterior.


## 2.2 Descartando ciertos outliers

Para hacer esta parte, se uso SQL directamente en el CLI de SQLite3. Se uso la siguiente query:
```SQL
CREATE VIEW ComunaLimpia AS
SELECT comuna.id, comuna.nombre FROM comuna
INNER JOIN poblacion
ON comuna.id = poblacion.cod
WHERE poblacion.habitantes > 5000
AND comuna.nombre <> "LAS CONDES";
```

Y para comprobar que efectivamente posee una cantidad correcta de filas:

```SQL
SELECT COUNT(*) FROM ComunaLimpia;
```

