package util;

import java.util.HashMap;
import java.util.Map;

public enum Parametro {
	TIPO_PROFILE("profile"),
	TIPO_HOME("home"),
	TIPO_LAST("last"),
	ANTES_DE("antesde"),
	PROXIMO("proximo"),
	APP("app"),
	USUARIO("usuario"),
	TIPO("tipo"),
	PER_PAGE("per_page"),
	DESCRICAO("descricao"),
	ID("id"), 
	CORPO("corpo");
	
	private static final Map<String, Parametro> mapToEnum = new HashMap<String, Parametro>();
	
	static{
		for(Parametro tipo: values()){
			mapToEnum.put(tipo.value(), tipo);
		}
	}
	
	private final String tipo;

	Parametro(String tipo) {
		this.tipo = tipo;
	}
	
	public String value(){
		return this.tipo;
	}
	
	public static Parametro getInstance(String value){
		return mapToEnum.get(value);
	}

}
