-- terrible confusing statements everywhere. :X
-- TODO: ask for clarification in person in OH
-- TODO: Q3, Q7 clarification
-- TODO: python validation

-- Q1
-- clarification: https://piazza.com/class/lm89trv0gfv1yl/post/64

SELECT DISTINCT job_title
FROM jobs
WHERE max_salary >= 5000 AND min_salary <= 4000;
-- "Administration Assistant"
-- "Human Resources Representative"
-- Programmer
-- "Marketing Representative"
-- "Purchasing Clerk"
-- "Shipping Clerk"
-- "Stock Clerk"

-- Q2

SELECT d.department_name, COUNT(*) AS department_size
FROM employees e
INNER JOIN departments d
  ON e.department_id = d.department_id
GROUP BY e.department_id
ORDER BY COUNT(*) DESC;
-- Shipping,7
-- Finance,6
-- Sales,6
-- Purchasing,6
-- IT,5
-- Executive,3
-- Accounting,2
-- Marketing,2
-- "Public Relations",1
-- "Human Resources",1
-- Administration,1


-- Q3
-- clarification: https://piazza.com/class/lm89trv0gfv1yl/post/68

SELECT d.department_name, AVG(j.max_salary)
FROM departments d
INNER JOIN employees e
  ON d.department_id = e.department_id
INNER JOIN jobs j
  ON e.job_id = j.job_id
GROUP BY d.department_id
HAVING AVG(j.max_salary) > 8000;
-- Marketing|12000.0
-- Human Resources|9000.0
-- IT|10000.0
-- Public Relations|10500.0
-- Sales|14666.6666666667
-- Executive|33333.3333333333
-- Finance|10166.6666666667
-- Accounting|12500.0



-- Q4

select count(*) from employees where department_id = (select department_id from departments where department_name = 'Shipping');
-- output: '7'


SELECT COUNT(*)
FROM employees e
INNER JOIN departments d
  ON e.department_id = d.department_id
WHERE d.department_name = 'Shipping';
-- output: 7


-- Q5

SELECT c.country_name
FROM countries c, regions r
WHERE c.region_id = r.region_id
  AND r.region_name = 'Europe';

-- Belgium
-- Switzerland
-- Germany
-- Denmark
-- France
-- Italy
-- Netherlands
-- United Kingdom


SELECT c.country_name
FROM countries c
INNER JOIN regions r
  ON r.region_id = c.region_id
WHERE r.region_name = 'Europe';

-- OUTPUT: the same

-- Q6

SELECT COUNT(*)
FROM employees e, departments d, locations l, countries c, regions r
WHERE e.department_id = d.department_id 
  AND d.location_id = l.location_id 
  AND l.country_id = c.country_id 
  AND c.region_id = r.region_id 
  AND r.region_name = 'Europe';

-- output 8

-- Q7

SELECT e1.first_name, e2.first_name
FROM employees e1, employees e2
WHERE e1.employee_id <> e2.employee_id 
  AND e1.department_id = e2.department_id 
  AND e1.manager_id = e2.manager_id
  AND e1.salary > 10000
  AND e2.salary > 10000
  AND e1.salary >= e2.salary;

-- **Q** By saying "have a salary exceeding 10000". Do we mean that both salaries should greater than 10000 or at least one is greater than 10000?
-- **Q** Do we want pairs of distinct employees? Or pairs of identical employees are allowed?
-- **Q** Should we have both `(p,q)` and `(q,p)` in the output if both of the pairs satisfy the conditions?
-- **Q** if employee a has salary 1000k, and employee b have salary 1000k
-- then (a,b) and (b,a) are both returned?

-- Q8

select salary, manager_id from employees where salary = (select min(salary) from employees);
-- output: '2500.0,114'

SELECT manager_id, salary
FROM employees
ORDER BY salary ASC
LIMIT 1;
-- OUTPUT: 114|2500.0


-- Q9

select department_name from departments where department_id = (select department_id from employees where salary = (select MAX(salary) from employees));

-- output: 'Executive'

SELECT d.department_name
FROM employees e
INNER JOIN departments d
  ON e.department_id = d.department_id
ORDER BY e.salary DESC
LIMIT 1;

-- output: 'Executive'

-- Q10
select employee_id from employees where employee_id not in (select employee_id from dependents);
-- OUTPUT:
-- 120
-- 121
-- 122
-- 123
-- 126
-- 177
-- 178
-- 179
-- 192
-- 193

SELECT e.employee_id
FROM employees e
LEFT JOIN dependents d
  ON d.employee_id = e.employee_id
WHERE d.dependent_id IS NULL;

-- OUTPUT:
-- 120
-- 121
-- 122
-- 123
-- 126
-- 177
-- 178
-- 179
-- 192
-- 193

