# What to Do?
For this stage, your project team will do the following:

1) Identify an application that needs to use a database. For example, suppose you want to build an application to keep track of the videos in a video rental business, the customers of the business, and their rentals. This application will need to use a database to store the various pieces of the above information. Other examples include a book selling application (e.g., amazon.com), a real estate application that lists houses for sale (e.g., realestate.com), an application that allows students to enroll in courses, etc.

2) Design an ER diagram for this application. The ER diagram should capture all entities and relationships you want to eventually store in your relational database. This ER diagram should contain at least:
    - one 1-1 relationship
    - one 1-many relationship
    - one weak entity set
    - one is-a hierarchy.

Try to write down as many domain constraints as possible that you think will be applicable to your application. Clearly identify all keys of all entity sets, as well as the primary keys of the entity sets.

A question we often got is how complex this ER diagram should be. There is no hard rule (it depends after all on your application). But roughly, you should aim for at least 4-6 entity sets and 4-8 relationships. Fewer than this, and your application may be too trivial. 

3) Translate the above ER diagram to a relational schema, using the various translation rules we will discuss in the class. When translating an is-a hierarchy, recall that you can translate it in at least three different ways. Decide on a way to translate and explain why you select that way.

Note that when you do the translation, it is okay to write a table schema like this: Students(sid, name, age, gpa). You don't have to write the full SQL query, such as CREATE TABLE STUDENTS ... 

# What to Submit
You need to create a pdf file that contains all of the above requested information. A good way to create this file is to use a Google doc, which allows all team members to easily collaborate online. If you have trouble drawing the ER diagram directly in the Google doc, you can draw the diagram *neatly* on a piece of paper, take a smart phone picture, then insert the picture into the doc. Finally, you can download a copy of the doc in pdf format. 

You do not have to use Google doc. We accept files edited in any tool, as long as the files are pdf. If you do not submit in pdf format, your group will loose points. 

In this pdf file, please include the following: 
- the names of all members of the group. 
- one paragraph describing the application you have selected. 
- the ER diagram for this application. 
- the translation of this ER diagram into a set of table schemas. 

Only one pdf file should be submitted per project group. Each project group has a name, such as G1, G2, etc. You should find out what is the name of your group: click on "People" in the Canvas page for 564 and you should find the group name. Suppose your group name is G6. (Note: TAs are still entering the groups into Canvas. So wait a few days before checking Canvas for your group name.)

Then name your pdf file G6.pdf, and upload it to Canvas. Each group should have just ONE member uploading this file. To upload, click on "Assignments", then on Project Stage 1. You should see a button that allows you to upload the file. 

The above instruction is for a fictional group named G6. You should modify it accordingly, using your true group name. 

Grading
We will evaluate your work based on conceptual cleanness, how well you adhere to good practice of ER design (as discussed in the class), how well you translate ER diagram into relational schema, and how well you present both the ER diagram and relational schema.
