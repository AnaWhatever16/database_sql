package series;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;

//algo_ miembros de la clase
//_algo elementos de entrada en f (privadas en este caso)
//las clases empiezan con mayuscula y cada palabra con mayuscula
//los miembros de las clases con minuscula la primera, el resto de palabras mayusculas
//las funciones son miembros de la clase!
//los constructores tienen el nombre de la clase (tienen datos de entrada etc)


public class SeriesDatabase {
	private static Connection conn_ = null;
	
	public SeriesDatabase() {
		
	}
    
	public boolean openConnection() {
		if(conn_ == null) {
			try {
				String drv = "com.mysql.cj.jdbc.Driver";
				Class.forName(drv);
				System.out.println("Driver cargado correctamente");
			} catch (ClassNotFoundException _e){
				System.err.println("Error, el driver no se ha encontrado");
				_e.printStackTrace();
				return false;
			} catch(Exception _e) {
				System.err.println("Error inesperado " + _e.getMessage());
				return false;
			}
			
			try {
				String serverAddress = "localhost:3306";
				String db = "series";
				String user = "series_user";
		        String pass = "series_pass";
				String url = "jdbc:mysql://" + serverAddress + "/" + db;
				conn_ = DriverManager.getConnection(url, user, pass);					
				System.out.println("Base de Datos cargada correctamente");
				return true;
			}catch (SQLException _e) {
				conn_ = null;
				System.err.println("Error, la base de datos no se ha encontrado");
				_e.printStackTrace();
				return false;
			} catch(Exception _e) {
				System.err.println("Error inesperado " + _e.getMessage());
				return false;
			}
		}else {
			System.out.println("Base de Datos cargada anteriormente"); // warning
			return false;
		}
	}

	public boolean closeConnection() {
		if(conn_ != null) {
			try {
				conn_.close();	
				conn_ = null;
				System.out.println("Base de Datos cerrada correctamente");
				return true;
			} catch(SQLException _e) {
				System.err.println("Error, la base de datos no se ha encontrado");
				_e.printStackTrace();
				return false;
			} catch(Exception _e) {
				System.err.println("Error inesperado " + _e.getMessage());
				return false;
			}			
		}else {
			return true;
		}
	}
	
	private boolean checkIfTableExists(String _table) {
		DatabaseMetaData db = null;
		ResultSet rs = null;
		boolean exist = false;
		try {
			db = conn_.getMetaData();
			rs = db.getTables(null, null, _table, new String[] {"TABLE"});
			exist = rs.next();
		} catch (SQLException e) {
			System.err.println("Error al buscar la tabla " + _table);
		} catch(Exception _e) {
			System.err.println("Error inesperado " + _e.getMessage());
		}finally {
			try {
				if(rs!=null) rs.close();				
			}catch(SQLException _e) {
				System.err.println("Error al cerrar ResultSet");
			}
		}
		return exist;
	}
	
	private boolean handleTableCreation(String _query, String _tableName) {
		openConnection();
		if(conn_ != null) {
			if(!checkIfTableExists(_tableName)) {
				Statement st = null;
				try {
					st = conn_.createStatement();
					st.executeUpdate(_query);
					System.out.println("Peticion de creacion de tabla " + _tableName + " realizada");
				}catch (SQLException _e) {
					System.err.println("Error al crear tabla " + _tableName);
				} catch(Exception _e) {
					System.err.println("Error inesperado " + _e.getMessage());
				}finally {
					try {
						if(st!=null) st.close();
					}catch(SQLException _e){
						System.err.println("Error al cerrar Statement");
					}
				}
				return checkIfTableExists(_tableName);
			}else{
				System.out.println("Tabla " + _tableName + " ya existe"); // warning
				return false;
			}
		}else {
			System.err.println("No hay conexion abierta");
			return false;
		}
	}

	public boolean createTableCapitulo() {
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
		return handleTableCreation(query, "capitulo");
	}

	public boolean createTableValora() {
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
		return handleTableCreation(query, "valora");
	}
	
	private java.sql.Date stringToDate(String _string){
		String sDate = _string.replace("/", " "); 
		sDate = sDate.replace("-", " ");			
		sDate = sDate.replace(".", " ");			
		sDate = sDate.replace("_", " ");
		String[] vDate = sDate.split(" ");
		SimpleDateFormat format = null;
		if(vDate[0].length() == 4) {
			format = new SimpleDateFormat("yyyy MM dd");
		}else if(vDate[2].length() == 4) {
			format = new SimpleDateFormat("dd MM yyyy");
		}else {
			System.err.println("Formato de fecha invalido");
			return null;
		}
        Date parsedDate = null;
		try {
			parsedDate = format.parse(sDate);
		} catch (ParseException e) {
			System.err.println("Formato de fecha invalido");
			return null;
		}
		
        java.sql.Date sqlDate = new java.sql.Date(parsedDate.getTime());	
        return sqlDate;
	}
	
	private int loadDataToTable(String _fileName, String _table) {
		int rowInserted = 0;
		openConnection();
		if(conn_ != null) {
			BufferedReader csvReader = null;
			PreparedStatement pst = null;			
			try {
				csvReader = new BufferedReader(new FileReader(_fileName));
				String row = csvReader.readLine();
				String tableElements = row.replace(";", ", ");
				int count = row.split(";").length;
				String qms = "";
				for(int i = 0; i < count; i++) {
					qms += "?";
					if(i != count-1) qms += ",";
				}
				String query = 	"INSERT INTO " + _table + "(" +
								tableElements + ") " + 
								"VALUES(" + qms + ");";
				conn_.setAutoCommit(false);
				pst = conn_.prepareStatement(query);
				boolean wrongDate = false;
				while ((row = csvReader.readLine()) != null) {
				    String[] data = row.split(";");
				    if(_table == "capitulo") { // HardCoded because we know what are the tables
				    	pst.setInt(1, Integer.parseInt(data[0]));
				    	pst.setInt(2, Integer.parseInt(data[1]));
				    	pst.setInt(3, Integer.parseInt(data[2]));
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
				    	java.sql.Date sqlDate = stringToDate(data[4]);
				    	if(sqlDate != null) {
				    		pst.setDate(5, sqlDate);				    		
				    	}else {
				    		wrongDate = true;
				    		break;
				    	}
				    	pst.setInt(6, Integer.parseInt(data[5]));				    		
				    }else {
				    	System.err.println("Solo se pueden insertar datos a las tablas capitulo o valora");
				    	System.err.println("Si se desean insertar datos a otras tablas,");
				    	System.err.println("a�adir otro else if para a�adir datos al preparedStatement en esta funci�n");
				    	break;
				    }
				    pst.executeUpdate();
				    rowInserted++;
				}
				if(!wrongDate) {
					conn_.commit();				
				}else {
					System.err.println("Rollback debido a fallo con el formato de fechas");
					conn_.rollback();
					rowInserted = 0;
				}
			} catch (FileNotFoundException _e) {
				System.err.println("El archivo no existe");
			}catch (IOException _e) {
				System.err.println("Fallo en la apertura del csv");
			} catch (SQLException _e) {
				System.err.println("Fallo con los PreparedStatement o haciendo Commit. Hacemos Rollback");
				System.err.println("Posibles fallos:");
				System.err.println("-> Tabla " + _table + " no creada");
				System.err.println("-> Alguno de los datos ha sido previamente insertado");
				try {
					conn_.rollback();
					rowInserted = 0;
				} catch (SQLException e) {
					System.err.println("Fallo haciendo rollback");
				}
			} catch(Exception _e) {
				System.err.println("Error inesperado: " + _e.getMessage() + ". Hacemos Rollback");
				try {
					conn_.rollback();
					rowInserted = 0;
				} catch (SQLException e) {
					System.err.println("Fallo haciendo rollback");
				}
			} finally {
				try {
					if(csvReader !=null) csvReader.close();
					if(pst != null) pst.close();
					conn_.setAutoCommit(true);
				} catch (IOException _e) {
					System.err.println("Fallo al cerrar el archivo");
				} catch (SQLException _e) {
					System.err.println("Fallo al cerrar el Statement");
				}
			}
		}else {
			System.err.println("No hay conexion abierta");
		}
		return rowInserted * 6;
	}

	public int loadCapitulos(String fileName) {
		return loadDataToTable(fileName, "capitulo");
	}

	public int loadValoraciones(String fileName) {
		return loadDataToTable(fileName, "valora");
	}
	
	private String cleanString(String _string) {
		String result = _string.replace(" ", "_");
		result = result.replace("'", "");
		return result;
	}

	public String catalogo() {
		openConnection();
		if(conn_ != null) {
			String seriesYTemps = "{";
			Statement st = null; // declaracion de la consulta 
			ResultSet rs = null; //resultados de la consulta
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
					titulo = cleanString(titulo);
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
				System.err.println("Problemas con la Statement");
				seriesYTemps = null;
			} catch(Exception _e) {
				System.err.println("Error inesperado: " + _e.getMessage());
				seriesYTemps = null;
			} finally {
				try {
					if(st != null) st.close();
					if(rs != null) rs.close();
				} catch (SQLException _e) {
					System.err.println("Fallo al cerrar el Statement");
					seriesYTemps = null;
				}
			}
			return seriesYTemps;

		}else {
			System.err.println("No hay conexion abierta");
			return null;
		}
	}
	
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
					String nombre = rs.getString("nombre");			nombre = cleanString(nombre);
					String apellido1 = rs.getString("apellido1");	apellido1 = cleanString(apellido1);
					String apellido2 = rs.getString("apellido2");	apellido2 = cleanString(apellido2);
					noCommentUsers += nombre + " " + apellido1 + " " + apellido2;
					primeraTupla = false;
				}
				noCommentUsers += "]";
			} catch (SQLException _e) {
				System.err.println("Problemas con la Statement");
				noCommentUsers = null;
			} catch(Exception _e) {
				System.err.println("Error inesperado: " + _e.getMessage());
				noCommentUsers = null;
			} finally {
				try {
					if(st != null) st.close();
					if(rs != null) rs.close();
				} catch (SQLException _e) {
					System.err.println("Fallo al cerrar el Statement");
					noCommentUsers = null;
				}
			}
			return noCommentUsers;

		}else {
			System.err.println("No hay conexion abierta");
			return null;
		}
	}

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
				System.err.println("Problemas con la Statement");
				media = -2.0;
			} catch(Exception _e) {
				System.err.println("Error inesperado: " + _e.getMessage());
				media = -2.0;
			} finally {
				try {
					if(pst1 != null) pst1.close();
					if(rs1 != null) rs1.close();
					if(pst2 != null) pst2.close();
					if(rs2 != null) rs2.close();
				} catch (SQLException _e) {
					System.err.println("Fallo al cerrar el Statement");
					media = -2.0;
				}
			}
			return media;
		}else {
			System.err.println("No hay conexion abierta");
			return -2.0;
		}
	}
	
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
				System.err.println("Problemas con la Statement");
				media = -2.0;
			} catch(Exception _e) {
				System.err.println("Error inesperado: " + _e.getMessage());
				media = -2.0;
			} finally {
				try {
					if(pst != null) pst.close();
					if(rs != null) rs.close();
				} catch (SQLException _e) {
					System.err.println("Fallo al cerrar el Statement");
					media = -2.0;
				}
			}
			return media;
		}else {
			System.err.println("No hay conexion abierta");
			return -2.0;
		}
	}

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
					System.out.println("Imagen a�adida correctamente");
				}else {
					System.err.println("Imagen no a�adida");
					System.err.println("Si no ha saltado una excepci�n, los posibles fallos son:");
					System.err.println("--> Imagen ya insertada en usuario");
					System.err.println("--> No existe un usuario con apellido 'Cabeza'");
					System.err.println("--> Existe m�s de un usuario apellidado 'Cabeza'");
				}
			}catch (SQLException _e) {
				System.err.println("Problemas con la Statement");	
			}catch (FileNotFoundException _e) {
				System.err.println("Fallo al abrir el archivo");
			} catch(Exception _e) {
				System.err.println("Error inesperado: " + _e.getMessage());	
			} finally {
				try {
					if(pst != null) pst.close();
				} catch (SQLException _e) {
					System.err.println("Fallo al cerrar el Statement");
				}
			}
			return success;
		}else {
			System.err.println("No hay conexion abierta");
			return false;
		}
	}
}