/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
        // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
        initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::sendMsg(Address *from, Address *to, MsgTypes msgType) {
        MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    size_t msgsize = sizeof(MessageHdr) + sizeof(from->addr) + sizeof(long) + 1;
    msg = (MessageHdr *) malloc(msgsize * sizeof(char));

    // create JOINREQ message: format of data is {struct Address myaddr}
    msg->msgType = JOINREP;
    memcpy((char *)(msg+1), &(from->addr), sizeof(from->addr));
    memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
    sprintf(s, "Trying to send joinrep...");
    log->LOG(&memberNode->addr, s);
#endif

    // send JOINREP message to introducer member
    emulNet->ENsend(from, to, (char *)msg, msgsize);

    free(msg);

    return 1;

}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
        char temp[2000];
        Member* m = (Member*) env; // Receiving Node
        //en_msg *em = (en_msg*) data;; // Sending Node
        int nid; // neighbour id
        short nport; // neighbour port

        Address recvedFromAddr;

        MessageHdr* mhdr = (MessageHdr*)data;
        Address *sendingAddr = (Address*)(data + sizeof(MessageHdr));

        memcpy(&(recvedFromAddr.addr), &(m->addr), sizeof(m->addr));
        //memcpy(&(sendingAddr.addr), &(em->from.addr), sizeof(em->from.addr));

        nid = *(int*)&sendingAddr->addr[0];
        nport =  *(short *)&sendingAddr->addr[4];
        //MemberListEntry* nle new MemberListEntry(nid,nport);
        MemberListEntry nle,ml;
        std::vector<MemberListEntry>::iterator it;

        #ifdef DEBUGLOG
        //sprintf(temp, "Receiving  msg %s from node %d.%d.%d.%d:%d queue which was sent by node %d.%d.%d.%d:%d", MsgTypeString[(int)mhdr->msgType],recvedFromAddr.addr[0], recvedFromAddr.addr[1], recvedFromAddr.addr[2], recvedFromAddr.addr[3], *(short *)&recvedFromAddr.addr[4], sendingAddr->addr[0], sendingAddr->addr[1], sendingAddr->addr[2], sendingAddr->addr[3], *(short *)&sendingAddr->addr[4]);
        sprintf(temp, "Receiving  msg %d from node %d.%d.%d.%d:%d queue which was sent by node %d.%d.%d.%d:%d", (int)mhdr->msgType,recvedFromAddr.addr[0], recvedFromAddr.addr[1], recvedFromAddr.addr[2], recvedFromAddr.addr[3], *(short *)&recvedFromAddr.addr[4], sendingAddr->addr[0], sendingAddr->addr[1], sendingAddr->addr[2], sendingAddr->addr[3], *(short *)&sendingAddr->addr[4]);
        log->LOG(&memberNode->addr, temp);
        #endif
        int nbr = 0;

        
        switch (mhdr->msgType) {

           case JOINREQ :
              nle = m->memberList.at(nid);
              nle.setid(nid);
              nle.setport(nport);
              m->memberList.at(nid) = nle;
             
              m->nnb++;

              for (unsigned int i = 0; i < m->memberList.size(); i++)
              {
                 ml =  m->memberList.at(i);
                 if (ml.getid() != -1)               
                   nbr++;
              }
              

              std::cout << "recived JOINREQ from " << nid << ":" << nport << " neighbours:" <<  m->nnb << " nbrs:" << nbr << endl;
              
              sendMsg(&recvedFromAddr,sendingAddr,JOINREP);
              break;

           case JOINREP :
              memberNode->inGroup = 1;
      //        *nle = m->memberList.at(nid);
        //      nle->setid(nid);
         //     nle->setport(nport);
          //    m->memberList.at(nid) = *nle;
              //m->memberList.insert(it+nid,*nle);
              nle = m->memberList.at(nid);
              nle.setid(nid);
              nle.setport(nport);
              m->memberList.at(nid) = nle;
              m->nnb++;

              for (unsigned int i = 0; i < m->memberList.size(); i++)
              {
                 ml =  m->memberList.at(i);
                 if (ml.getid() != -1)               
                   nbr++;
              }

              std::cout << "recived JOINREP from " << nid << ":" << nport << " neighbours:" <<  m->nnb << " nbrs:" << nbr << endl;
              break;

           default:
              cout << "recived message not recognized" << endl;

           break;
        }


        return false;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

int MP1Node::getMaxPeers() {
       return par->MAX_NNB;
}
/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
        // reserve space in vector for all peers
        memberNode->memberList.reserve(getMaxPeers());
	memberNode->memberList.clear();
        std::vector<MemberListEntry>::iterator it =  memberNode->memberList.begin();
        for ( int i=0; i <= getMaxPeers(); i++) {
           MemberListEntry* nle = new MemberListEntry(-1,-1);
           memberNode->memberList.insert(it+i,*nle);
        } 
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
