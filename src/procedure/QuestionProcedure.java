package procedure;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import org.omg.CORBA.PRIVATE_MEMBER;

import basic.FileOps;

/**
 * Split question texts and remove entity mentions
 * @author kren
 *
 */
public class QuestionProcedure {

	private String questionFile = "./data/questions.txt";
	private String markedEntityFile = "./data/mark_entity.txt";
	
	private void questionSpliter() {
		LinkedList<String> questions = FileOps.LoadFilebyLine(questionFile);
		LinkedList<String> questionsSplit = FileOps.LoadFilebyLine(questionFile);
		LinkedList<String> questionsMarked = FileOps.LoadFilebyLine(markedEntityFile);
		
		int count = 0;
//		Iterator<String> qIterator = questions.iterator();
		Iterator<String> qmIterator = questionsMarked.iterator();
		
		while(qmIterator.hasNext()) {
			String qmLine = qmIterator.next();
			String[] qmItems = qmLine.split("\t");
			int qId = Integer.parseInt(qmItems[0]);
			int beginOffset = Integer.parseInt(qmItems[1]);
			int endOffset = Integer.parseInt(qmItems[2]);
			String question = questions.get(qId-1);
			String questionSplit = question.substring(0, beginOffset)+question.substring(endOffset+1, question.length());
			questionsSplit.remove(qId-1);
			questionsSplit.add(qId-1, questionSplit);
		}
		FileOps.SaveList("./data/questions_splitted.txt", questionsSplit);
	}
	
	public static void main(String[] args) {
		QuestionProcedure p = new QuestionProcedure();
		p.questionSpliter();
	}

}
