package com.example.batchprocessing;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PersonController {
	
	private static final Logger log = LoggerFactory.getLogger(PersonController.class);


	@GetMapping("/persons")
	public List<Person> getPersonList() {
	
		List<Person> persons = PersonRowMapper.personList;
		log.info("In controller to get list of person");
		return persons;
	}
	
	@GetMapping("/")
	public String defaultMapping() {
	
		return "PersonList";
	}
	
}
