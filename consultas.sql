# Consulta de createTableCapitulo
CREATE TABLE capitulo(
n_orden INT,
titulo VARCHAR(100),
duracion INT,
fecha_estreno DATE,
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

# Consulta de loadCapitulos
# Se añaden de manera dinámica en el código las entradas de la tabla
INSERT INTO capitulo(id_serie, n_temporada, n_orden, fecha_estreno, titulo, duracion) VALUE (?,?,?,?,?,?);

# Consulta de loadValoraciones
INSERT INTO valora(id_serie, n_temporada, n_orden, id_usuario, fecha, valor) VALUE (?,?,?,?,?,?);

# Consulta catalogo
SELECT s.titulo, t.n_capitulos   
FROM serie s LEFT JOIN temporada t ON s.id_serie=t.id_serie  
ORDER BY s.id_serie ASC, t.n_temporada ASC;

# Consulta noHanComentado
SELECT u.nombre, u.apellido1, u.apellido2  
FROM usuario u LEFT JOIN comenta c ON u.id_usuario=c.id_usuario  
WHERE c.texto IS NULL  
ORDER BY u.apellido1 ASC, u.apellido2 ASC, u.nombre ASC;

# Consultas mediaGenero
SELECT descripcion FROM genero WHERE descripcion = ?;

SELECT AVG(v.valor) AS media  
FROM capitulo c   
INNER JOIN pertenece p ON c.id_serie=p.id_serie  
INNER JOIN genero g ON p.id_genero=g.id_genero  
INNER JOIN valora v ON (c.id_serie, c.n_orden, c.n_temporada) = (v.id_serie, v.n_orden, v.n_temporada)  
WHERE g.descripcion = ?;

# Consulta duracionMedia
SELECT AVG(c.duracion) AS media  
FROM capitulo c  
INNER JOIN serie s ON c.id_serie = s.id_serie  
LEFT JOIN valora v ON (c.id_serie, c.n_orden, c.n_temporada) = (v.id_serie, v.n_orden, v.n_temporada)  
WHERE s.idioma = ? AND v.valor IS NULL;

# Consultas setFoto
SELECT COUNT(nombre) AS cuenta  
FROM usuario  
WHERE apellido1 = 'Cabeza';

UPDATE usuario   
SET fotografia = ?  
WHERE apellido1 = 'Cabeza' AND fotografia IS NULL;








