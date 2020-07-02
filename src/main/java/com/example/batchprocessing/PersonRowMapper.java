package com.example.batchprocessing;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.jdbc.core.RowMapper;

public class PersonRowMapper implements RowMapper{
	
	static List<Person> personList = new ArrayList<>();

	@Override
	public Object mapRow(ResultSet rs, int rowNum) throws SQLException {

		Person person = new Person();
		person.setFirstName(rs.getString("first_name"));
		person.setLastName(rs.getString("last_name"));
		personList.add(person);
		return person;
	}

}
