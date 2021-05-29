// Práctica 2: SQL y Java
// Autoras: Ana María Casado Faulí 	- Matrícula: 20A003
//          Alicia Germán Bellod	- Matrícula: 20A059

package series;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

// En esta clase se encontrarán primero las funciones públicas que se pidieron
// en el enunciado de la práctica, en el orden original y luego se tienen las
// funciones privadas que se utilizan en las primeras.

public class SeriesDatabase {
	// La conexión a la base de datos se declara como miembro estático de la clase
	// para asegurar que será un objeto único que podremos utilizar a lo largo de la ejecución.
	// Además se ha declarado como privado para evitar el acceso desde el exterior.
	private static Connection conn_ = null;
	
	// Constructor vacío.
	public SeriesDatabase() {}
	
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////FUNCIONES PÚBLICAS///////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
    ////////////////////////////////////////////////////////////////////
	/////////////////////FUNCIÓN 1: OPEN CONNECTION/////////////////////
	////////////////////////////////////////////////////////////////////
	// Función encargada de abrir la conexión con la base de datos.
	// Este método se llamará en el resto de las funciones que requieran 
	// acceso a la base de datos para asegurar que existe.
	// Devuelve true si la conexión se realiza y false si no se realiza
	// o si ya se encontraba abierta.
	
	public boolean openConnection() {
		if(conn_ == null) { // Si la conexión está cerrada.
			try { // Intentamos cargar el driver
				String drv = "com.mysql.cj.jdbc.Driver";
				Class.forName(drv);
				System.out.println("[INFO] Driver cargado correctamente");
			} catch (ClassNotFoundException _e){
				System.err.println("[EXCEPTION] Error, el driver no se ha encontrado");
				_e.printStackTrace();
				return false;
			} catch(Exception _e) {
				System.err.println("[EXCEPTION] Error inesperado " + _e.getMessage());
				return false;
			}
			
			try { // Intentamos abrir la base de datos
				String serverAddress = "localhost:3306";
				String db = "series";
				String user = "series_user";
		        String pass = "series_pass";
				String url = "jdbc:mysql://" + serverAddress + "/" + db;
				conn_ = DriverManager.getConnection(url, user, pass);					
				System.out.println("[INFO] Base de Datos cargada correctamente");
				return true;
			}catch (SQLException _e) {
				conn_ = null; // para asegurar que la conexión no exista
				System.err.println("[EXCEPTION] Error, la base de datos no se ha encontrado");
				return false;
			} catch(Exception _e) {
				System.err.println("[EXCEPTION] Error inesperado " + _e.getMessage());
				return false;
			}
		}else { // Si la conexión ya estaba abierta
			System.out.println("[WARNING] Base de Datos cargada anteriormente");
			return false;
		}
	}
	
	/////////////////////////////////////////////////////////////////////
	/////////////////////FUNCIÓN 2: CLOSE CONNECTION/////////////////////
	/////////////////////////////////////////////////////////////////////
	// Función que cierra la conexión. Devuelve true si se cierra o
	// si ya se encontraba cerrada. Devuelve false si hay alguna excepción.
	
	public boolean closeConnection() {
		if(conn_ != null) { // Si la conexión está abierta
			try { // Intentamos cerrar la conexión
				conn_.close();	
				conn_ = null;
				System.out.println("[INFO] Base de Datos cerrada correctamente");
				return true;
			} catch(SQLException _e) {
				System.err.println("[EXCEPTION] Error, error cerrando la conexión");
				return false;
			} catch(Exception _e) {
				System.err.println("[EXCEPTION] Error inesperado " + _e.getMessage());
				return false;
			}			
		}else { // Si la conexión ya estaba cerrada
			System.out.println("[WARNING] La conexión ya se encontraba cerrada");
			return true;
		}
	}
	
	//////////////////////////////////////////////////////////////////////////
	/////////////////////FUNCIÓN 3: CREATE TABLE CAPITULO/////////////////////
	//////////////////////////////////////////////////////////////////////////
	// Creación de la tabla capítulo.
	// Devuelve false si la tabla no se ha podido crear (por algún tipo de error o porque
	// ya existiese) y true si la tabla se ha creado correctamente.
	
	public boolean createTableCapitulo() {
		// Query SQL en formato String
		// La PK de capítulo tiene las PK de serie y temporada
		// (que por construcción existen ambas en temporada).
		// Las actualizaciones se hacen en cascada tanto para 
		// borrado como en actualización.
		String query = 	"CREATE TABLE capitulo (" + 
						"n_orden INT, " +
						"titulo VARCHAR(100), " + 
						"duracion INT, " +
						"fecha_estreno DATE, " +
						"id_serie INT, " +
						"n_temporada INT, " + 
						"PRIMARY KEY (id_serie, n_temporada, n_orden), " +
						"FOREIGN KEY (id_serie, n_temporada) REFERENCES temporada (id_serie, n_temporada) " +
						"ON DELETE CASCADE ON UPDATE CASCADE);"; 
		
		// Esta función se encarga de la creación completa de la tabla y devuelve lo mismo 
		// que debe devolver esta función.
		return handleTableCreation(query, "capitulo");
	}

	////////////////////////////////////////////////////////////////////////
	/////////////////////FUNCIÓN 4: CREATE TABLE VALORA/////////////////////
	////////////////////////////////////////////////////////////////////////
	// Creación de la tabla valora.
	// Devuelve false si la tabla no se ha podido crear (por algún tipo de error o porque
	// ya existiese) y true si la tabla se ha creado correctamente.
	
	public boolean createTableValora() {
		// Query SQL en formato String
		// La PK que viene de capítulo tiene las PK de serie y temporada.
		// Las actualizaciones se hacen en cascada tanto para 
		// borrado como en actualización.
		String query = 	"CREATE TABLE valora (" + 
						"id_serie INT, " +
					    "n_temporada INT, " +
						"n_orden INT, " +
						"id_usuario INT, " + 
						"valor INT, " +
						"fecha DATE, " + 
						"PRIMARY KEY (id_serie, n_temporada, n_orden, id_usuario, fecha), " +
						"FOREIGN KEY (id_serie, n_temporada, n_orden) REFERENCES capitulo (id_serie, n_temporada, n_orden) " +
						"ON DELETE CASCADE ON UPDATE CASCADE," +
						"FOREIGN KEY (id_usuario) REFERENCES usuario (id_usuario) " +
						"ON DELETE CASCADE ON UPDATE CASCADE);"; 
		// Esta función se encarga de la creación completa de la tabla y devuelve lo mismo 
		// que debe devolver esta función.
		return handleTableCreation(query, "valora");
	}

	//////////////////////////////////////////////////////////////////
	////////////////////FUNCIÓN 5: LOAD CAPITULOS/////////////////////
	//////////////////////////////////////////////////////////////////
	// Función que inserta las tuplas que vienen definidas en el archivo que se pasa
	// como parámetro. Devuelve la cantidad de elementos insertados en la tabla.
	public int loadCapitulos(String fileName) {
		// Función que se encarga de la gestión de los datos y devuelve lo mismo.
		return loadDataToTable(fileName, "capitulo");
	}
	
	/////////////////////////////////////////////////////////////////////
	////////////////////FUNCIÓN 6: LOAD VALORACIONES/////////////////////
	/////////////////////////////////////////////////////////////////////
	// Función que inserta las tuplas que vienen definidas en el archivo que se pasa
	// como parámetro. Devuelve la cantidad de elementos insertados en la tabla.
	public int loadValoraciones(String fileName) {
		// Función que se encarga de la gestión de los datos y devuelve lo mismo.
		return loadDataToTable(fileName, "valora");
	}

	////////////////////////////////////////////////////////////
	////////////////////FUNCIÓN 7: CATALOGO/////////////////////
	////////////////////////////////////////////////////////////
	public String catalogo() {
		openConnection();
		if(conn_ != null) {
			String seriesYTemps = "{";
			Statement st = null; 
			ResultSet rs = null; 
			String query = 	"SELECT s.titulo, t.n_capitulos " + 
							"FROM serie s LEFT JOIN temporada t ON s.id_serie=t.id_serie " +
							"ORDER BY s.id_serie ASC, t.n_temporada ASC;";
			try {
				st = conn_.createStatement(); 
				rs = st.executeQuery(query);
				String tituloOld = "";
				boolean primeraTupla = true;
				boolean primeraTemp = true;
				while(rs.next()) {
					String titulo = rs.getString("titulo");
					int capitulosTemp = rs.getInt("n_capitulos");
					if(!tituloOld.equals(titulo)) {
						if(!primeraTupla) {
							seriesYTemps += "],";
						}
						seriesYTemps += titulo + ":[";
						primeraTupla = false;
						primeraTemp = true;
					} 
					if(capitulosTemp !=0) {
						if(!primeraTemp) {
							seriesYTemps += ",";
						}
						String ctString = Integer.toString(capitulosTemp);
						seriesYTemps += ctString;
						primeraTemp = false;
					}					
					tituloOld = titulo;
				}
				if(!seriesYTemps.equals("{")) seriesYTemps += "]";
				seriesYTemps += "}";
			} catch (SQLException _e) {
				System.err.println("[EXCEPTION] Problemas con la Statement");
				seriesYTemps = null;
			} catch(Exception _e) {
				System.err.println("[EXCEPTION] Error inesperado: " + _e.getMessage());
				seriesYTemps = null;
			} finally {
				try {
					if(st != null) st.close();
					if(rs != null) rs.close();
				} catch (SQLException _e) {
					System.err.println("[EXCEPTION] Fallo al cerrar el Statement");
					seriesYTemps = null;
				}
			}
			return seriesYTemps;

		}else {
			System.err.println("[ERROR] No hay conexion abierta");
			return null;
		}
	}
	
	////////////////////////////////////////////////////////////////////
	////////////////////FUNCIÓN 8: NO HAN COMENTADO/////////////////////
	////////////////////////////////////////////////////////////////////
	public String noHanComentado() {
		openConnection();
		if(conn_ != null) {
			String query = 	"SELECT u.nombre, u.apellido1, u.apellido2 " +
							"FROM usuario u LEFT JOIN comenta c ON u.id_usuario=c.id_usuario " +
							"WHERE c.texto IS NULL " +
							"ORDER BY u.apellido1 ASC, u.apellido2 ASC, u.nombre ASC;";
			Statement st = null;
			ResultSet rs = null; 
			String noCommentUsers = "[";
			try {
				st = conn_.createStatement(); 
				rs = st.executeQuery(query);
				boolean primeraTupla = true;
				while(rs.next()) {
					if(!primeraTupla) {
						noCommentUsers += ", ";
					}
					String nombre = rs.getString("nombre");			
					String apellido1 = rs.getString("apellido1");	
					String apellido2 = rs.getString("apellido2");	
					noCommentUsers += nombre + " " + apellido1 + " " + apellido2;
					primeraTupla = false;
				}
				noCommentUsers += "]";
			} catch (SQLException _e) {
				System.err.println("[EXCEPTION] Problemas con la Statement");
				noCommentUsers = null;
			} catch(Exception _e) {
				System.err.println("[EXCEPTION] Error inesperado: " + _e.getMessage());
				noCommentUsers = null;
			} finally {
				try {
					if(st != null) st.close();
					if(rs != null) rs.close();
				} catch (SQLException _e) {
					System.err.println("[EXCEPTION] Fallo al cerrar el Statement");
					noCommentUsers = null;
				}
			}
			return noCommentUsers;

		}else {
			System.err.println("[ERROR] No hay conexion abierta");
			return null;
		}
	}
	////////////////////////////////////////////////////////////////
	////////////////////FUNCIÓN 9: MEDIA GENERO/////////////////////
	////////////////////////////////////////////////////////////////
	public double mediaGenero(String genero) {
		openConnection();
		if(conn_ != null) {
			String query1 = "SELECT descripcion FROM genero WHERE descripcion = ?;";
			String query2 = "SELECT AVG(v.valor) AS media " +
							"FROM capitulo c " + 
							"INNER JOIN pertenece p ON c.id_serie=p.id_serie " +
							"INNER JOIN genero g ON p.id_genero=g.id_genero " +
							"INNER JOIN valora v ON (c.id_serie, c.n_orden, c.n_temporada) = (v.id_serie, v.n_orden, v.n_temporada) " +
							"WHERE g.descripcion = ?;";
			PreparedStatement pst1 = null;
			PreparedStatement pst2 = null;
			ResultSet rs1 = null; 
			ResultSet rs2 = null;
			double media = -1.0;
			try {
				pst1 = conn_.prepareStatement(query1); 
				pst1.setString(1, genero);
				rs1 = pst1.executeQuery();
				if(!rs1.next()) {
					media = -1.0;
				}else {
					pst2 = conn_.prepareStatement(query2); 
					pst2.setString(1, genero);
					rs2 = pst2.executeQuery();
					if(rs2.next()) {
						media = rs2.getDouble("media");
					}else {
						media = 0.0;
					}
				}
			} catch (SQLException _e) {
				System.err.println("[EXCEPTION] Problemas con la Statement");
				media = -2.0;
			} catch(Exception _e) {
				System.err.println("[EXCEPTION] Error inesperado: " + _e.getMessage());
				media = -2.0;
			} finally {
				try {
					if(pst1 != null) pst1.close();
					if(rs1 != null) rs1.close();
					if(pst2 != null) pst2.close();
					if(rs2 != null) rs2.close();
				} catch (SQLException _e) {
					System.err.println("[EXCEPTION] Fallo al cerrar el Statement");
					media = -2.0;
				}
			}
			return media;
		}else {
			System.err.println("[ERROR] No hay conexion abierta");
			return -2.0;
		}
	}
	
	///////////////////////////////////////////////////////////////////
	////////////////////FUNCIÓN 10: DURACION MEDIA/////////////////////
	///////////////////////////////////////////////////////////////////
	public double duracionMedia(String idioma) {
		openConnection();
		if(conn_ != null) {
			String query = "SELECT AVG(c.duracion) AS media " +
							"FROM capitulo c " +
							"INNER JOIN serie s ON c.id_serie = s.id_serie " +
							"LEFT JOIN valora v ON (c.id_serie, c.n_orden, c.n_temporada) = (v.id_serie, v.n_orden, v.n_temporada) " +
							"WHERE s.idioma = ? AND v.valor IS NULL;";
			PreparedStatement pst = null;
			ResultSet rs = null;
			double media = -1.0;
			try {
				pst = conn_.prepareStatement(query); 
				pst.setString(1, idioma);
				rs = pst.executeQuery();
				if(!rs.next() || rs.getDouble("media") == 0) {
					media = -1.0;
				}else {
					media = rs.getDouble("media");
				}
			} catch (SQLException _e) {
				System.err.println("[EXCEPTION] Problemas con la Statement");
				media = -2.0;
			} catch(Exception _e) {
				System.err.println("[EXCEPTION] Error inesperado: " + _e.getMessage());
				media = -2.0;
			} finally {
				try {
					if(pst != null) pst.close();
					if(rs != null) rs.close();
				} catch (SQLException _e) {
					System.err.println("[EXCEPTION] Fallo al cerrar el Statement");
					media = -2.0;
				}
			}
			return media;
		}else {
			System.err.println("[ERROR] No hay conexion abierta");
			return -2.0;
		}
	}
	
	/////////////////////////////////////////////////////////////
	////////////////////FUNCIÓN 11: SET FOTO/////////////////////
	/////////////////////////////////////////////////////////////
	public boolean setFoto(String filename) {
		openConnection();
		if(conn_ != null) {
			String query1 = "SELECT COUNT(nombre) AS cuenta " +
							"FROM usuario " +
							"WHERE apellido1 = 'Cabeza';";
			String query2 = "UPDATE usuario " + 
							"SET fotografia = ? " +
							"WHERE apellido1 = 'Cabeza' AND fotografia IS NULL;";
			PreparedStatement pst = null;
			Statement st = null;
			ResultSet rs = null;
			boolean success = false;
			try {
				st = conn_.createStatement();
				rs = st.executeQuery(query1);
				rs.next();
				if(rs.getInt("cuenta")==1) {
					pst = conn_.prepareStatement(query2);
					File file = new File(filename); 
					FileInputStream fis = new FileInputStream(file);
					pst.setBinaryStream(1, fis, (int)file.length());
					int result = pst.executeUpdate();	
					if(result != 0) success = true;
				}
				if(success) {
					System.out.println("[INFO] Imagen añadida correctamente");
				}else {
					System.err.println("[ERROR] Imagen no añadida");
					System.err.println("Si no ha saltado una excepción, los posibles fallos son:");
					System.err.println("--> Imagen ya insertada en usuario");
					System.err.println("--> No existe un usuario con apellido 'Cabeza'");
					System.err.println("--> Existe más de un usuario apellidado 'Cabeza'");
				}
			}catch (SQLException _e) {
				System.err.println("[EXCEPTION] Problemas con la Statement");	
			}catch (FileNotFoundException _e) {
				System.err.println("[EXCEPTION] Fallo al abrir el archivo");
			} catch(Exception _e) {
				System.err.println("[EXCEPTION] Error inesperado: " + _e.getMessage());	
			} finally {
				try {
					if(pst != null) pst.close();
					if(st != null) st.close();
					if(rs != null) rs.close();
				} catch (SQLException _e) {
					System.err.println("[EXCEPTION] Fallo al cerrar el Statement");
				}
			}
			return success;
		}else {
			System.err.println("[ERROR] No hay conexion abierta");
			return false;
		}
	}

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////FUNCIONES PRIVADAS///////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	/////////////////////////////////////////////////////////////////////////
	//////////////////////////CHECK IF TABLE EXISTS//////////////////////////
	/////////////////////////////////////////////////////////////////////////
	// Función para comprobación de la existencia de una tabla en la base de datos
	// Devuelve true si la tabla existe y false si no existe o salta alguna excepción
	
	private boolean checkIfTableExists(String _table) {
		DatabaseMetaData db = null;
		ResultSet rs = null;
		boolean exist = false;
		try {
			db = conn_.getMetaData(); // Cogemos el MetaData
			rs = db.getTables(null, null, _table, new String[] {"TABLE"}); // Buscamos la tabla por su nombre
			exist = rs.next(); // Si existe se almacena en rs
		} catch (SQLException e) {
			System.err.println("[EXCEPTION]Error al buscar la tabla " + _table);
		} catch(Exception _e) {
			System.err.println("[EXCEPTION]Error inesperado " + _e.getMessage());
		}finally {
			try {
				//db no se tiene que cerrar.
				if(rs!=null) rs.close(); //Cerramos rs. 			
			}catch(SQLException _e) {
				System.err.println("[EXCEPTION]Error al cerrar ResultSet");
			}
		}
		return exist;
	}
	
	/////////////////////////////////////////////////////////////////////////
	///////////////////////////HANDLE TABLE CREATION/////////////////////////
	/////////////////////////////////////////////////////////////////////////
	// Función de creación de tablas. Tiene de entradas la query en formato string
	// de la creación de la tabla y otro string con el nombre de la tabla.
	
	private boolean handleTableCreation(String _query, String _tableName) {
		openConnection(); // Comprobamos la conexión a la base de datos
		if(conn_ != null) { // Comprobamos que la conexión esté abierta
			if(!checkIfTableExists(_tableName)) { // Si la tabla no existe
				// Ejecutamos la query de creación de tabla
				Statement st = null;
				try {
					st = conn_.createStatement();
					st.executeUpdate(_query); 
					System.out.println("[INFO] Peticion de creacion de tabla " + _tableName + " realizada");
				}catch (SQLException _e) {
					System.err.println("[EXCEPTION]Error al crear tabla " + _tableName);
				} catch(Exception _e) {
					System.err.println("[EXCEPTION]Error inesperado " + _e.getMessage());
				}finally {
					try {
						if(st!=null) st.close();
					}catch(SQLException _e){
						System.err.println("[EXCEPTION]Error al cerrar Statement");
					}
				}
				if(checkIfTableExists(_tableName)) { // Comprobamos de nuevo para ver si se ha realizado con existo
					System.out.println("[INFO] Tabla " + _tableName + " creada");
					return true;
				} else{
					System.err.println("[ERROR] La tabla " + _tableName + " no se ha creado");
					return false;
				}
			}else{ // Si la tabla existe
				System.out.println("[WARNING] Tabla " + _tableName + " ya existe");
				return false;
			}
		}else { // Si la conexión no existe
			System.err.println("[ERROR] No hay conexion abierta");
			return false;
		}
	}
	
	/////////////////////////////////////////////////////////////////////////
	/////////////////////////////STRING TO DATE//////////////////////////////
	/////////////////////////////////////////////////////////////////////////
	// Función para gestionar distintos tipos de formatos de fecha
	// a partir de un string. Esta función permite el soporte de 10 tipos de formatos.
	// Le entrará un string que debería contener una fecha.
	// Devuelve la fecha en formato java.sql.Date si está entre los 10 posibles
	// formatos o null si el formato es otro o no es una fecha.
	
	private java.sql.Date stringToDate(String _string){
		// En primer lugar eliminamos cualquier tipo de separador que se 
		// encuentre en el string y lo sustituimos por un espacio.
		String sDate = _string.replace("/", " "); 
		sDate = sDate.replace("-", " ");			
		sDate = sDate.replace(".", " ");			
		sDate = sDate.replace("_", " ");
		// Ponemos los 3 elementos de la fecha en un vector
		String[] vDate = sDate.split(" "); 
		// Format almacena el formato de fecha al que se parseará el string
		SimpleDateFormat format = null;
		// Si el primer elemento tiene 4 caractéres, ese será el año
		if(vDate[0].length() == 4) { 
			format = new SimpleDateFormat("yyyy MM dd");
		// Si el último elemento tiene 4 caractéres, ese será el año
		}else if(vDate[2].length() == 4) {
			format = new SimpleDateFormat("dd MM yyyy");
		// Sino no se encuentra el año en esta fecha
		}else {
			System.err.println("[ERROR] Formato de fecha invalido");
			return null;
		}
        Date parsedDate = null;
		try {
			// Usando el formato escogido se parse el string a java.util.Date
			parsedDate = format.parse(sDate);
		} catch (ParseException e) {
			System.err.println("[EXCEPTION] Formato de fecha invalido");
			return null;
		}
		
		// Parseo de java.ultil.Date a java.sql.Date
        java.sql.Date sqlDate = new java.sql.Date(parsedDate.getTime());	
        return sqlDate;
	}
	
	/////////////////////////////////////////////////////////////////////////
	///////////////////////////LOAD DATA TO TABLE////////////////////////////
	/////////////////////////////////////////////////////////////////////////
	// Función que se encarga de la gestión de la inserción de los datos en una tabla.
	// Devuelve la cantidad de elementos insertados.
	
	private int loadDataToTable(String _fileName, String _table) {
		int rowInserted = 0;
		openConnection(); // Comprobamos que la conexión esté abierta
		if(conn_ != null) { // Si está abierta
			BufferedReader csvReader = null;
			PreparedStatement pst = null;			
			try {
				csvReader = new BufferedReader(new FileReader(_fileName)); // Abrimos el archivo csv
				// Leemos la primera línea del csv donde se encuentran los nombres de las columnas.
				String row = csvReader.readLine(); 
				String tableElements = row.replace(";", ", ");
				// Añadimos tantos interrogantes a la PreparedStatement como 
				// columnas tenga la tabla
				int count = row.split(";").length;
				String qms = ""; 
				for(int i = 0; i < count; i++) {
					qms += "?";
					if(i != count-1) qms += ",";
				}
				
				// Preparamos el string de la query
				String query = 	"INSERT INTO " + _table + "(" +
								tableElements + ") " + 
								"VALUES(" + qms + ");";
				conn_.setAutoCommit(false); // Autocommit false para evitar inserciones en caso de error
				pst = conn_.prepareStatement(query); // Preparamos la statement
				boolean wrongDate = false;
				while ((row = csvReader.readLine()) != null) { // Leemos línea a línea el csv
				    String[] data = row.split(";");
				    // Esta parte es menos dinámica porque para poder usar la preparedStatement
				    // debemos conocer la forma de la tabla y los tipos. 
				    // Es por ello que el else final dice que si queremos meter otra tabla con este método
				    // debemos programarlo.
				    if(_table == "capitulo") {
				    	pst.setInt(1, Integer.parseInt(data[0]));
				    	pst.setInt(2, Integer.parseInt(data[1]));
				    	pst.setInt(3, Integer.parseInt(data[2]));
				    	// Comprobamos que la fecha tiene formato soportado
				    	java.sql.Date sqlDate = stringToDate(data[3]);
				    	if(sqlDate != null) {
				    		pst.setDate(4, sqlDate);				    		
				    	}else {
				    		wrongDate = true;
				    		break;
				    	}
				    	pst.setString(5, data[4]);
				    	pst.setInt(6, Integer.parseInt(data[5]));
				    }else if (_table == "valora") {
				    	pst.setInt(1, Integer.parseInt(data[0]));
				    	pst.setInt(2, Integer.parseInt(data[1]));
				    	pst.setInt(3, Integer.parseInt(data[2]));
				    	pst.setInt(4, Integer.parseInt(data[3]));
				    	// Comprobamos que la fecha tiene formato soportado
				    	java.sql.Date sqlDate = stringToDate(data[4]);
				    	if(sqlDate != null) {
				    		pst.setDate(5, sqlDate);				    		
				    	}else {
				    		wrongDate = true;
				    		break;
				    	}
				    	pst.setInt(6, Integer.parseInt(data[5]));				    		
				    }else {
				    	System.err.println("[ERROR] Solo se pueden insertar datos a las tablas capitulo o valora");
				    	System.err.println("Si se desean insertar datos a otras tablas, añadir otro else if");
				    	break;
				    }
				    pst.executeUpdate(); // En cada iteración ejecutamos la query con los nuevos datos
				    rowInserted++; // Contamos una fila en cada iteración
				}
				
				// Comprobamos si la salida del bucle while es debido a un formato de fecha invalido
				if(!wrongDate) { // Si ha ido todo bien 
					conn_.commit(); // Podemos hacer commit	
				}else { // Si el formato de fecha no está soportado
					System.err.println("[ERROR] Rollback debido a fallo con el formato de fechas");
					conn_.rollback();
					rowInserted = 0;
				}
			} catch (FileNotFoundException _e) {
				// No hacemos rollback ya que es anterior a la statement
				System.err.println("[EXCEPTION] El archivo no existe"); 
			}catch (IOException _e) {
				// No hacemos rollback ya que es anterior a la statement
				System.err.println("[EXCEPTION] Fallo en la apertura del csv");
			} catch (SQLException _e) {
				System.err.println("[EXCEPTION] Fallo con los PreparedStatement o haciendo Commit. Hacemos Rollback");
				System.err.println("Posibles fallos:");
				System.err.println("-> Tabla " + _table + " no creada");
				System.err.println("-> Alguno de los datos ha sido previamente insertado");
				try {
					conn_.rollback(); // Rollback porque ha habido algún error
					rowInserted = 0;
				} catch (SQLException e) {
					System.err.println("[EXCEPTION] Fallo haciendo rollback");
				}
			} catch(Exception _e) {
				System.err.println("[EXCEPTION] Error inesperado: " + _e.getMessage() + ". Hacemos Rollback");
				try {
					conn_.rollback(); // Rollback para evitar problemas
					rowInserted = 0;
				} catch (SQLException e) {
					System.err.println("[EXCEPTION] Fallo haciendo rollback");
				}
			} finally {
				try {
					// Cerramos los distintos elementos de la función
					if(csvReader !=null) csvReader.close();
					if(pst != null) pst.close();
					conn_.setAutoCommit(true); // Ponemos la config original (default) al autocommit
				} catch (IOException _e) {
					System.err.println("[EXCEPTION] Fallo al cerrar el archivo");
				} catch (SQLException _e) {
					System.err.println("[EXCEPTION] Fallo al cerrar el Statement");
				}
			}
		}else {
			System.err.println("[ERROR] No hay conexion abierta");
		}
		return rowInserted * 6; // Como tenemos 6 columnas en ambas tablas multiplicamos el número de inserciones por 6
	}
}