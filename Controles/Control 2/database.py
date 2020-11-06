''' 
This is our database
It manages the relations, queries and data
It has a single buffer that the relations can use to load the pages from disk
It checks that the relations are of correct type
'''

from pageNew import *
from buffer import *
from relation import *

# The buffer used by our database, and all of tis relations
Buffer = Buffer()

# The relation dictionary indexed by relation name
Relations = {}


# This is what 'CREATE TABLE R(a int, b int, c text)' does here attributes = ['a','b','c'], types = ['int','int','str']
# buff is the buffer that the table has access to; this is set to be the one used by the database
def create_table(rName, attributes, types, buff = Buffer):
    # Creates a relation with rName, and attributes as its attributes with respective types

    # First we check that the relation does not exist
    if (rName in Relations.keys()):
        print('Relation already exists')
        return False

    Rel = Relation(rName, attributes, types, buff)
    Relations[rName] = Rel

    # If all went well e return True
    return True


# This is what 'SELECT * FROM R' does
def all_tuples(R):
    R = Relations[R]

    R.open()

    while (R.has_next()):
        print(R.next())

    R.close()


# This is what 'SELECT * FROM R WHERE a = x' does
def filtered_tuples(R, cond):
    # Here cond is just a single attribute equality
    # cond is if the form 'attr = value'

    R = Relations[R]

    attr, value = cond.split('=')
    # remove trailing and leading spaces in case cond looks like 'attr = value'
    attr = attr.strip()
    value = value.strip()

    # Check if attr is in relation
    R_attributes = R.get_attribute_types()
    if not attr in R_attributes:
        print(f'{attr} is not a valid attribute for {R.rName}')
        return

    # If type is integer, convert (else is string)
    if R_attributes[attr] == 'int':
        if value.isdecimal():
            value = int(value)
        else:
            print(f'{value} is not an int')
            return

    # Check every tuple
    R.open()
    while (R.has_next()):
        tup = R.next()
        if R.get_individual_values(tup)[attr] == value:
            print(tup)
    R.close()


def cross(R, S):
    # Performs the cross product of the two relations
    # Assumes that R is not equal to S, otherwise it will not work properly -- for bonus, implement iterators properly


    R = Relations[R].get_iterator()
    S = Relations[S].get_iterator()

    R.open()

    while (R.has_next()):
        tupR = R.next()
        S.open()
        while (S.has_next()):
            print(tupR, S.next())
        S.close()

    R.close()

    # End of the cross product
    return True


# SELECT * FROM R,S WHERE R.attrib = S.attrib
def nested_loop_join(R, S, attrib):
    # Performs a join on the attribute attrib
    R = Relations[R]
    S = Relations[S]

    # Check if attr is in relation
    R_attributes = R.get_attribute_types()
    S_attributes = S.get_attribute_types()
    if not attrib in R_attributes:
        print(f'{attrib} is not a valid attribute for {R.rName}')
        return
    if not attrib in S_attributes:
        print(f'{attrib} is not a valid attribute for {S.rName}')
        return

    # Check every tuple combination
    R.open()
    while (R.has_next()):
        tupR = R.next()
        S.open()
        while (S.has_next()):
            tupS = S.next()
            if R.get_individual_values(tupR)[attrib] == S.get_individual_values(tupS)[attrib]:
                print(tupR,tupS)
        S.close()
    R.close()


# SELECT * FROM R,S WHERE R.attrib = S.attrib
def block_nested_loop_join(R,S,attrib):
    # For bonus b)
    R = Relations[R]
    S = Relations[S]

    # Check if attr is in relation
    R_attributes = R.get_attribute_types()
    S_attributes = S.get_attribute_types()
    if not attrib in R_attributes:
        print(f'{attrib} is not a valid attribute for {R.rName}')
        return
    if not attrib in S_attributes:
        print(f'{attrib} is not a valid attribute for {S.rName}')
        return

    # Check every tuple combination
    # NOTE: All relations share same buffer
    # Fill every frame of the buffer but one with R pages
    R.buffer.flush_and_fill(R.root_page, R.buffer.buffSize - 1)
    next_R_page = R.buffer.frames[R.buffer.buffSize - 2].next
    # Fill last frame with S page
    R.buffer.fetch_into_frame(S.root_page, R.buffer.buffSize - 1)
    next_S_page = R.buffer.frames[R.buffer.buffSize - 1].next
    for i in R.buffer.frames:
        print(i.pname)

    print(next_R_page, next_S_page)

    S.open()
    while True:
        tupS = S.next()
        R.open()
        while (True):
            tupR = R.next()
            if S.get_individual_values(tupS)[attrib] == R.get_individual_values(tupR)[attrib]:
                print(tupS,tupR)
            if R.current_page == next_R_page:
                break
        if S.current_page == next_S_page:
            S_next_page = S.current_page.get_next_page()
            R_next_page = R.current_page.get_next_page()
            if S_next_page:
                R.fetch_into_frame(S_next_page, R.buffer.buffSize - 1)
            elif R_next_page:
                R.buffer.flush_and_fill(R.current_page, R.buffer.buffSize - 1)
            else:
                break

    S.close()
    R.close()


# Test cases:
if __name__ == "__main__":
    create_table('R',['a','b'],['int','int'])

    create_table('S',['b','c'],['int','int'])

    # cross('R', 'R')

    # all_tuples('R')

    # all_tuples('S')

    # filtered_tuples('R', 'a = 1')

    # nested_loop_join('R', 'S', 'b')

    block_nested_loop_join('R', 'S', 'b')
