#include <iostream>
#ifndef STACK_H
#define STACK_H
using namespace std;
// this is the node class used to build up the LIFO stack
template <class Data>// here data is input parameter
class Node {

private:

	Data holdMe;
	Node *next;

public:

	/*****************************************/
	/** WHATEVER CODE YOU NEED TO ADD HERE!! */
	/*****************************************/
    Data getData() {
        return holdMe;
    }

    void setData(Data data) {
        holdMe = data;
    }

    Node* getNext() {
        return next;
    }

    void setNext(Node* nextNode) {
        next = nextNode;
    }
};

// a simple LIFO stack
template <class Data>
class Stack {

	Node<Data> *head;

public:

	// destroys the stack
	~Stack () { /* your code here */
        Node<Data> *cur = head;
        while (cur != NULL) {
            Node<Data> *next = cur->getNext();
            delete cur;
            cur = next;
        }
	}

	// creates an empty stack
	Stack () {
        head = NULL;
	}

	// adds pushMe to the top of the stack
	void push (Data data) {
//	    cout << data << endl;
        Node<Data> *newHead = new Node<Data>;
        newHead->setData(data);
        newHead->setNext(head);
//        cout << newHead->getData() << endl;
        head = newHead;
//        cout << head->getData() << endl;
	}

	// return true if there are not any items in the stack
	bool isEmpty () {
        return head == NULL;
	}

	// pops the item on the top of the stack off, returning it...
	// if the stack is empty, the behavior is undefined
	Data pop () { /* replace with your code */
        if (head != NULL) {
            Node<Data> *cur = head;
            Data data = head->getData();
            head = head->getNext();
            delete cur;
            return data;
        }
        else {
            return Data();
        }
	}
};

#endif
