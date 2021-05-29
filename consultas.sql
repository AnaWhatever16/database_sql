# Consulta de createTableCapitulo
CREATE TABLE capitulo(
n_orden INT,
titulo VARCHAR(100),
duracion INT,
id_serie INT,
n_temporada INT,
PRIMARY KEY (id_serie, n_temporada, n_orden),
FOREIGN KEY (id_serie, n_temporada) REFERENCES temporada (id_serie, n_temporada)
ON DELETE CASCADE ON UPDATE CASCADE);

# Consulta de createTableValora
CREATE TABLE valora(
id_serie INT,
n_temporada INT,
n_orden INT,
id_usuario INT,
valor INT,
fecha DATE,
PRIMARY KEY (id_serie, n_temporada, n_orden, id_usuario, fecha),
FOREIGN KEY (id_serie, n_temporada, n_orden) REFERENCES temporada (id_serie, n_temporada, n_orden)
ON DELETE CASCADE ON UPDATE CASCADE,
FOREIGN KEY (id_usuario) REFERENCES usuario (id_usuario) 
ON DELETE CASCADE ON UPDATE CASCADE);







