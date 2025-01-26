<ins>**Problem Description:**</ins>

Write a solution to swap the seat id of every two consecutive students.

If the number of students is odd, the id of the last student is not swapped.

<ins>**DDL:**</ins>

CREATE TABLE seats (
    id INT,
    student VARCHAR(10)
);

INSERT INTO seats VALUES 
(1, 'Amit'),
(2, 'Deepa'),
(3, 'Rohit'),
(4, 'Anjali'),
(5, 'Neha'),
(6, 'Sanjay'),
(7, 'Priya');
