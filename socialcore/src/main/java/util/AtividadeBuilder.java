package util;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import socialcore.activitymanager.exceptions.RecordException;
import socialcore.activitymanager.model.Atividade;
import socialcore.activitymanager.model.Objeto;
import socialcore.activitymanager.model.Resultado;
import socialcore.resources.exceptions.InternalServerErrorException;

public class AtividadeBuilder {
	
	public static final int TOTAL_ACTIVITIES = 100;
	public static final int TOTAL_APPS = 5;
	public static final int TOTAL_USERS = 10;

	private Long usuario = 111L;
	private Long app = 1L;
	private Timestamp createdAt = new Timestamp(new Date().getTime());
	private String tipo = "comentar";
	private Resultado resultado;
	private Objeto objeto;

	private AtividadeBuilder() {}
	
	public static AtividadeBuilder anAtividade() {
        return new AtividadeBuilder();
    }

	public Atividade build() {
		Atividade atividade = new Atividade(usuario, app, tipo, createdAt, null, null);
		resultado = (resultado != null) ? resultado : getMensagemDefault();
		atividade.setResultado(resultado);
		
		objeto = (objeto != null) ? objeto : getObjetoDefault();
		atividade.setObjeto(objeto);
		atividade.saved();
        return atividade;
    }
	
	public Atividade create() throws RecordException, InternalServerErrorException {
		Atividade atividade = build();
		atividade.save();
        return atividade;
    }
	
	public AtividadeBuilder withUsuario(Long usuario){
        this.usuario = usuario;
        return this;
    }

	public AtividadeBuilder withApp(Long app){
        this.app = app;
        return this;
    }

	public AtividadeBuilder withCorpo(String mensagem){
		resultado = getMensagemDefault();
		resultado.addFlexibleField(Parametro.CORPO.value(), mensagem);
        return this;
    }
	
	public AtividadeBuilder empty(){
		app = null;
		createdAt = null;
		tipo = null;
		usuario = null;
        return this;
    }
	
	public AtividadeBuilder withCreatedAt(Timestamp desde){
		createdAt = desde;
        return this;
    }
	
	private Resultado getMensagemDefault(){
		Resultado resultado = new Resultado();
		resultado.addFlexibleField(Parametro.DESCRICAO.value(), "Mensagem default");
		resultado.setId("http://anotations.api.abril.com.br/comentarios/45");
		resultado.setTipo("comentario");
		return resultado;
	}
	
	private Objeto getObjetoDefault(){
		Objeto objeto = new Objeto();
		objeto.addFlexibleField(Parametro.DESCRICAO.value(), "materia sobre o cine sesc");
		objeto.setId("http://vejasp.abril.com.br/cinema/cinema-na-mesa-menu-cinesesc");
		objeto.setTipo("materia");
		return objeto;
	}
	
	public static void populate(){
		int count = 1;
		
		try {
			for(int atividadeIndex = 1; atividadeIndex <= TOTAL_ACTIVITIES; atividadeIndex++){
				for(long app = 1; app <= TOTAL_APPS; app++){
					for(long usuario = 1; usuario <= TOTAL_USERS; usuario++){
						count++;
						
						Atividade atividade = AtividadeBuilder.anAtividade().withApp(app).withUsuario(usuario).build();
						atividade.propagated();
						GregorianCalendar gregorianCalendar = new GregorianCalendar();
						gregorianCalendar.add(Calendar.MINUTE, count);
						atividade.setPublishedAt(new Timestamp(gregorianCalendar.getTimeInMillis()));
						atividade.save();
						
						System.out.println("Published At: " + atividade.getPublishedAt().getTime());
					}
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
