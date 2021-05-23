package series;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class SeriesDatabase {
	private boolean dbOpen_;
	private Connection conn;
	
	public SeriesDatabase() {
		dbOpen_ = false;
	}

	public boolean openConnection() {
		if(!dbOpen_) {
			try {
				String drv = "com.mysql.cj.jdbc.Driver";
				Class.forName(drv);
				System.out.println("Driver cargado correctamente");
			} catch (ClassNotFoundException e){
				System.out.println("Error, el driver no se ha encontrado");
				e.printStackTrace();
				return false;
			}
			
			try {
				String serverAddress = "localhost:3306";
				String db = "series";
				String user = "series_user";
		        String pass = "series_pass";
				String url = "jdbc:mysql://" + serverAddress + "/" + db;
				conn = DriverManager.getConnection(url, user, pass);					
				dbOpen_ = true;
				System.out.println("Base de Datos cargada correctamente");
				return true;
			}catch (SQLException e) {
				System.out.println("Error, la base de datos no se ha encontrado");
				e.printStackTrace();
				return false;
			}
		}else {
			return false;
		}
	}

	public boolean closeConnection() {
		if(dbOpen_) {
			try {
				conn.close();	
				System.out.println("Base de Datos cerrada correctamente");
				return true;
			} catch(SQLException e) {
				System.out.println("Error, la base de datos no se ha encontrado");
				return false;
			}			
		}else {
			return true;
		}
	}

	public boolean createTableCapitulo() {
		openConnection();
		System.out.println("Hello World");
		return false;
	}

	public boolean createTableValora() {
		return false;
	}

	public int loadCapitulos(String fileName) {
		return 0;
	}

	public int loadValoraciones(String fileName) {
		return 0;
	}

	public String catalogo() {
		return null;
	}
	
	public String noHanComentado() {
		return null;
	}

	public double mediaGenero(String genero) {
		return 0.0;
	}
	
	public double duracionMedia(String idioma) {
		return 0.0;
	}

	public boolean setFoto(String filename) {
		return false;
	}

}
