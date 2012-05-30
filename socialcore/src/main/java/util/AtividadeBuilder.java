package util;

import java.sql.Timestamp;
import java.util.Date;

import socialcore.activitymanager.exceptions.RecordException;
import socialcore.activitymanager.model.Atividade;
import socialcore.activitymanager.model.Objeto;
import socialcore.activitymanager.model.Resultado;
import socialcore.resources.exceptions.InternalServerErrorException;

public class AtividadeBuilder {
	
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
		atividade.validateAndSave();
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
		
		try {
			for(int atividadeIndex = 1; atividadeIndex <= 100; atividadeIndex++){
				for(int app = 1; app <= 5; app++){
					for(int usuario = 1; usuario <= 20; usuario++){
						Atividade atividade = AtividadeBuilder.anAtividade().withApp(1L).withUsuario(100L).build();
						atividade.save();
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
