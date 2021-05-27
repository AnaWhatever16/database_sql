package series;

import java.io.BufferedReader;
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
import java.text.SimpleDateFormat;

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
				while ((row = csvReader.readLine()) != null) {
				    String[] data = row.split(";");
				    if(_table == "capitulo") { // HardCoded because we know what are the tables
				    	pst.setInt(1, Integer.parseInt(data[0]));
				    	pst.setInt(2, Integer.parseInt(data[1]));
				    	pst.setInt(3, Integer.parseInt(data[2]));
				    	SimpleDateFormat format = new SimpleDateFormat("dd/MM/yyyy");
				        Date parsed = format.parse(data[3]);
				        java.sql.Date sqlDate = new java.sql.Date(parsed.getTime());
				    	pst.setDate(4, sqlDate);
				    	pst.setString(5, data[4]);
				    	pst.setInt(6, Integer.parseInt(data[5]));
				    }else if (_table == "valora") {
				    	pst.setInt(1, Integer.parseInt(data[0]));
				    	pst.setInt(2, Integer.parseInt(data[1]));
				    	pst.setInt(3, Integer.parseInt(data[2]));
				    	pst.setInt(4, Integer.parseInt(data[3]));
				    	SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
				        Date parsed = format.parse(data[4]);
				        java.sql.Date sqlDate = new java.sql.Date(parsed.getTime());
				    	pst.setDate(5, sqlDate);
				    	pst.setInt(6, Integer.parseInt(data[5]));
				    }
				    pst.executeUpdate();
				    rowInserted++;
				}
				conn_.commit();				
			} catch (FileNotFoundException _e) {
				System.err.println("El archivo no existe");
			}catch (IOException _e) {
				System.err.println("Fallo en la apertura del csv");
			} catch (SQLException _e) {
				System.err.println("Fallo con los PreparedStatement. Hacemos Rollback");
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
				System.err.println("Error inesperado " + _e.getMessage() + ". Hacemos Rollback");
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

	public String catalogo() {
		openConnection();
		if(conn_ != null) {
			System.out.println("Hello World");
		}
		return null;
	}
	
	public String noHanComentado() {
		openConnection();
		if(conn_ != null) {
			System.out.println("Hello World");
		}
		return null;
	}

	public double mediaGenero(String genero) {
		openConnection();
		if(conn_ != null) {
			System.out.println("Hello World");
		}
		return 0.0;
	}
	
	public double duracionMedia(String idioma) {
		openConnection();
		if(conn_ != null) {
			System.out.println("Hello World");
		}
		return 0.0;
	}

	public boolean setFoto(String filename) {
		openConnection();
		if(conn_ != null) {
			System.out.println("Hello World");
		}
		return false;
	}
	
}
