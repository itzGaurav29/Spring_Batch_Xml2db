package com.example.batchprocessing;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

public class Map2ListProcessor  implements ItemProcessor<Person, List<Person>>{

	private static final Logger log = LoggerFactory.getLogger(Map2ListProcessor.class);
	
	static List<Person> personList = new ArrayList<Person>();
	
	@Override
	public List<Person> process(Person person) throws Exception {

		personList.add(person);
		log.info("Adding (" + person + ") to ArrayList");
		return personList;
	}

}
