package series;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

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
				System.out.println("Error, el driver no se ha encontrado");
				_e.printStackTrace();
				return false;
			} catch(Exception _e) {
				System.out.println("Error inesperado " + _e.getMessage());
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
				System.out.println("Error, la base de datos no se ha encontrado");
				_e.printStackTrace();
				return false;
			} catch(Exception _e) {
				System.out.println("Error inesperado " + _e.getMessage());
				return false;
			}
		}else {
			System.out.println("Base de Datos cargada anteriormente");
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
				System.out.println("Error, la base de datos no se ha encontrado");
				_e.printStackTrace();
				return false;
			} catch(Exception _e) {
				System.out.println("Error inesperado " + _e.getMessage());
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
			System.out.println("Error al buscar la tabla " + _table);
		} catch(Exception _e) {
			System.out.println("Error inesperado " + _e.getMessage());
		}finally {
			try {
				if(rs!=null) rs.close();				
			}catch(SQLException _e) {
				System.out.println("Error al cerrar ResultSet");
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
					System.out.println("Error al crear tabla " + _tableName);
				} catch(Exception _e) {
					System.out.println("Error inesperado " + _e.getMessage());
				}finally {
					try {
						if(st!=null) st.close();
					}catch(SQLException _e){
						System.out.println("Error al cerrar Statement");
					}
				}
				
				return checkIfTableExists(_tableName);
			}else{
				System.out.println("Tabla " + _tableName + " existe");
				return false;
			}
		}else {
			System.out.println("No hay conexion abierta");
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

	public int loadCapitulos(String fileName) {
		openConnection();
		if(conn_ != null) {
			System.out.println("Hello World");
		}
		return 0;
	}

	public int loadValoraciones(String fileName) {
		openConnection();
		if(conn_ != null) {
			System.out.println("Hello World");
		}
		return 0;
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
