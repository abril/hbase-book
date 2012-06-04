package util;

import java.util.Comparator;

import socialcore.activitymanager.model.Atividade;

public class AtividadeComparator implements Comparator<Atividade> {

	public int compare(Atividade arg0, Atividade arg1) {
		
		long publishedAtInMilliseconds = arg0.getPublishedAt().getTime();
		long publishedAtInMillisecondsToCompare = arg1.getPublishedAt().getTime();
		
		if(publishedAtInMilliseconds == publishedAtInMillisecondsToCompare){
			return 0;
		} else if (publishedAtInMilliseconds < publishedAtInMillisecondsToCompare){
			return 1;
		} else {
			return -1;
		}
	}

}
