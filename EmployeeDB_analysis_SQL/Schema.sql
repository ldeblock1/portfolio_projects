CREATE TABLE category(
	category_id VARCHAR(30),
	category VARCHAR(30),
	PRIMARY KEY (category_id)
);
SELECT * FROM category_id;

CREATE TABLE contacts(
	contact_id INT NOT NULL,
	name VARCHAR(30),
	email VARCHAR(50),
	PRIMARY KEY contact_id
);
SELECT * FROM contacts;

CREATE TABLE subcategory(
	subcategory_id VARCHAR(8),
	subcategory VARCHAR(30),
	PRIMARY KEY (subcategory_id)
);

CREATE TABLE campaign(
	cf_id INT NOT NULL,
	contact_id INT NOT NULL,
	company_name VARCHAR(30),
	description VARCHAR(60),
	goal INT NOT NULL,
	pledged INT NOT NULL,
	outcome VARCHAR(30),
	backers_count INT NOT NULL,
	country VARCHAR(30),
	currency VARCHAR(5),
	launch_date VARCHAR(9),
	end_date VARCHAR(9),
	category_id VARCHAR(10),
	subcategory_id VARCHAR(30),
	PRIMARY KEY (cf_id),
	FOREIGN KEY (subcategory_id) REFERENCES subcategory(subcategory_id),
	FOREIGN KEY (contact_id) REFERENCES contacts(contact_id),
	FOREIGN KEY (category_id) REFERENCES category(category_id)
);

CREATE TABLE crowdfunding(
	cf_id INT NOT NULL,
	contact_id INT NOT NULL,
	blurb VARCHAR(30),
	goal INT NOT NULL,
	pledged INT NOT NULL,
	outcome VARCHAR(30),
	backers_count INT NOT NULL,
	country VARCHAR (30),
	currency VARCHAR(5),
	launched_at INT NOT NULL,
	deadline INT NOT NULL,
	staff_pick BOOLEAN, 
	spotlight BOOLEAN,
	category&sub-category,
	PRIMARY KEY (cf_id),
	FOREIGN KEY (contact_id) REFERENCES contacts(contact_id)
);
