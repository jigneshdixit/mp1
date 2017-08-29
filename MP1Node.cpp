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
        int nid; 
        int nport; 
        MemberListEntry mle;
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        nid = *(int*)&memberNode->addr.addr[0];
        nport =  *(short *)&memberNode->addr.addr[4];
        mle = memberNode->memberList.at(nid);
        mle.setid(nid);
        mle.setport(nport);
        mle.settimestamp(par->getcurrtime());
        mle.setheartbeat(0);
        memberNode->memberList.at(nid) = mle;
        memberNode->inGroup = true;

    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

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
int MP1Node::sendMsg(Address *from, Address *to, MsgTypes msgType, int size, MemberListEntry **data) {
        MessageHdr *msg;
        MemberListEntry* mle = *data;
#ifdef DEBUGLOG
    static char s[1024];
#endif
    cout << "**data:" << data << " *data:" << *data << endl;
    cout << "sizeof(MemberListEntry*:" << sizeof(MemberListEntry*)<< endl;

    size_t msgsize = sizeof(MessageHdr) + sizeof(from->addr) + sizeof(long) + sizeof(MemberListEntry*) + sizeof(int) + 1;
    msg = (MessageHdr *) malloc(msgsize * sizeof(char));

    // create JOINREP message: format of data is {struct Address myaddr}
    msg->msgType = JOINREP;
    memcpy((char *)(msg+1), &(from->addr), sizeof(from->addr));
    memcpy((char *)(msg+1) + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
    memcpy((char *)(msg+1) + sizeof(memberNode->addr.addr) +sizeof(long), &(mle), sizeof(MemberListEntry*));
    memcpy((char *)(msg+1) + sizeof(memberNode->addr.addr) +sizeof(long)+sizeof(MemberListEntry*),&size,sizeof(int));

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
        int rid;

        Address recvedFromAddr;

        MessageHdr* mhdr = (MessageHdr*)data;
        Address *sendingAddr = (Address*)(data + sizeof(MessageHdr));
        long *hb = (long*)(data + sizeof(MessageHdr)+ sizeof (m->addr));

        memcpy(&(recvedFromAddr.addr), &(m->addr), sizeof(m->addr));
        //memcpy(&(sendingAddr.addr), &(em->from.addr), sizeof(em->from.addr));

        nid = *(int*)&sendingAddr->addr[0];
        nport =  *(short *)&sendingAddr->addr[4];
        rid = *(int*)&recvedFromAddr.addr[0];
        
        //MemberListEntry* nle new MemberListEntry(nid,nport);
        MemberListEntry nle,ml;
        std::vector<MemberListEntry>::iterator it;

        #ifdef DEBUGLOG
        //sprintf(temp, "Receiving  msg %s from node %d.%d.%d.%d:%d queue which was sent by node %d.%d.%d.%d:%d", MsgTypeString[(int)mhdr->msgType],recvedFromAddr.addr[0], recvedFromAddr.addr[1], recvedFromAddr.addr[2], recvedFromAddr.addr[3], *(short *)&recvedFromAddr.addr[4], sendingAddr->addr[0], sendingAddr->addr[1], sendingAddr->addr[2], sendingAddr->addr[3], *(short *)&sendingAddr->addr[4]);
        sprintf(temp, "Receiving  msg %d from node %d.%d.%d.%d:%d queue which was sent by node %d.%d.%d.%d:%d", (int)mhdr->msgType,recvedFromAddr.addr[0], recvedFromAddr.addr[1], recvedFromAddr.addr[2], recvedFromAddr.addr[3], *(short *)&recvedFromAddr.addr[4], sendingAddr->addr[0], sendingAddr->addr[1], sendingAddr->addr[2], sendingAddr->addr[3], *(short *)&sendingAddr->addr[4]);
        log->LOG(&memberNode->addr, temp);
        #endif
        int nbr = 0;
        MemberListEntry *mle;
        MemberListEntry **mymle;
        unsigned char* chp;
        int *mysize;
        
        switch (mhdr->msgType) {

           case JOINREQ :
              
              // Update Member Entry
              nle = m->memberList.at(nid);
              nle.setid(nid);
              nle.settimestamp(par->getcurrtime());
              nle.setheartbeat(0);
              nle.setport(nport);
              m->memberList.at(nid) = nle;
             
              m->nnb++;
              mle = m->memberList.data();
              

//              for (unsigned int i = 0; i < m->memberList.size(); i++)
//              {
//                 ml =  m->memberList.at(i);
//                 if (ml.getid() != -1)               
//                   nbr++;
//              }
              

              cout << "Node " << rid << " member list dump:::" << endl;
              for (unsigned int i = 0; i < m->memberList.size(); i++)
              {
                 cout << i << ":" << mle->getid() << "heartbeat " << mle->gettimestamp() << endl;
                 mle++;
              }
              

              mle = m->memberList.data();

              std::cout << "recived JOINREQ from " << nid << ":" << nport << " neighbours:" <<  m->nnb << " nbrs:" << nbr << "data:" << mle << endl;
              
              sendMsg(&recvedFromAddr,sendingAddr,JOINREP,m->memberList.size(),&mle);
              break;

           case JOINREP :

              mymle = (MemberListEntry**) (data + sizeof(MessageHdr)+ sizeof (m->addr) + sizeof(long));
              mle = *mymle;
              mysize = (int*)(data + sizeof(MessageHdr)+ sizeof (m->addr) + sizeof(long) + sizeof(MemberListEntry*));
              cout << "size:" << *mysize << " mydata:" << mle << endl;

              memberNode->inGroup = 1;
 //             nle = m->memberList.at(nid);
 //             nle.setid(nid);
 //             nle.setport(nport);
 //             m->memberList.at(nid) = nle;
 //             m->nnb++;
              int id; 
              cout << "Node " << rid << " member list dump:::" << endl;
             // update memberListEntry from recived ones
             for (unsigned int i = 0; i < *mysize; i++)
             {
                id = mle->getid();
                if (id != -1)  {             
                   nle = m->memberList.at(id);
                   nle.setid(id);
                   nle.setport(mle->getport());
                   nle.settimestamp(par->getcurrtime());
                   nle.setheartbeat(mle->getheartbeat());
                   // TODO update timesmap and heartbeat
                   m->memberList.at(id) = nle;
                  
                   m->nnb++;
                }
                cout << i << ":::" << mle->getid() << " timestamp " << mle->gettimestamp() << endl;
                
                mle++;
             }

              std::cout << "recived JOINREP from " << nid << ":" << nport << " neighbours:" <<  m->nnb << " nbrs:" << nbr << endl;
              break;

           case HEARTBEAT :
            {
        #ifdef DEBUGLOG
              MemberListEntry entry;
              for (unsigned int i = 0; i < m->memberList.size(); i++)
              {
                 entry = m->memberList.at(i);
                 sprintf(temp,"entry %d id:%d heartbeat:%u timestamp:%u", i, entry.getid(), entry.getheartbeat(), entry.gettimestamp());
                 log->LOG(&memberNode->addr, temp);
              }
        #endif
              mymle = (MemberListEntry**) (data + sizeof(MessageHdr)+ sizeof (m->addr) + sizeof(long));
              mle = *mymle;
              mysize = (int*)(data + sizeof(MessageHdr)+ sizeof (m->addr) + sizeof(long) + sizeof(MemberListEntry*));

              std::cout << "recived HEARTBEAT from " << nid << ":" << nport << endl;
              cout << "Node " << rid << " member list dump:::" << endl;
              cout << "size:" << *mysize << " mydata:" << mle << endl;
             // update memberListEntry from recived ones
             // merge heartbeat and timestamp line by line
             int id;
             for (unsigned int i = 0; i < *mysize; i++)
             {
                id = mle->getid();
                nle = m->memberList.at(i);
                //if (nle.getid()!=-1)  {             
                if (id != -1)  {             
                   if (mle->getheartbeat() > nle.getheartbeat()) {
                      nle.setid(id); 
                      nle.setheartbeat(mle->getheartbeat()); 
                      nle.settimestamp(par->getcurrtime()); 
                      m->memberList.at(i) = nle;
                   }
                }
                cout << i << ":::" << mle->getid() << " heartbeat:" << mle->getheartbeat() << " timestamp:" << mle->gettimestamp() << endl;
                
                mle++;
             }
              
        #ifdef DEBUGLOG
              for (unsigned int i = 0; i < m->memberList.size(); i++)
              {
                 entry = m->memberList.at(i);
                 sprintf(temp,"exit %d id:%d heartbeat:%u timestamp:%u", i, entry.getid(), entry.getheartbeat(), entry.gettimestamp());
                 log->LOG(&memberNode->addr, temp);
              }
        #endif
#if 0
              int gossipIndex;
              MemberListEntry* mmle;
              mmle = memberNode->memberList.data();
              // send gossip message to another 2 nodes
              for (int gossipCount = 0; gossipCount < 2; gossipCount++) {
                 do 
                 {
                    gossipIndex = rand() % (par->MAX_NNB+1);
       
                 } while ((gossipIndex==0) || (gossipIndex == nid) );
                 
                 Address to = getAddressFromId(gossipIndex);
                 sendMsgHeartbeat(&memberNode->addr, &to, HEARTBEAT, memberNode->heartbeat, 100, &mmle,memberNode->memberList.size());
              }
#endif
            }  
            break;

           default:
              cout << "recived message not recognized" << endl;

           break;
        }


        return false;
}

int MP1Node::sendMsgHeartbeat(Address *from, Address *to, MsgTypes msgType, long heartbeat, long ts, MemberListEntry **data, int size) {
        MessageHdr *msg;
        MemberListEntry* mle = *data;
        int toNodeId = *(int*)&to->addr[0];
#ifdef DEBUGLOG
    static char s[1024];
#endif
    cout << "**data:" << data << " *data:" << *data << endl;

    size_t msgsize = sizeof(MessageHdr) + sizeof(from->addr) + sizeof(long) + sizeof(MemberListEntry*) + sizeof(int) + 1;
    msg = (MessageHdr *) malloc(msgsize * sizeof(char));

    // create HEARTBEAT message: format of data is {struct Address myaddr}
    msg->msgType = HEARTBEAT;
    memcpy((char *)(msg+1), &(from->addr), sizeof(from->addr));
    memcpy((char *)(msg+1) + sizeof(memberNode->addr.addr), &heartbeat, sizeof(long));
    memcpy((char *)(msg+1) + sizeof(memberNode->addr.addr) +sizeof(long), &(mle), sizeof(MemberListEntry*));
    memcpy((char *)(msg+1) + sizeof(memberNode->addr.addr) +sizeof(long)+sizeof(MemberListEntry*),&size,sizeof(int));

#ifdef DEBUGLOG
    sprintf(s, "Trying to send heartbeat to node ID %d ...",toNodeId);
    log->LOG(&memberNode->addr, s);
#endif

    // send JOINREP message to introducer member
    emulNet->ENsend(from, to, (char *)msg, msgsize);

    free(msg);

    return 1;

}


/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
        int nid = *(int*)&memberNode->addr.addr[0];
        MemberListEntry* mmle;
        int gossipIndex;
        MemberListEntry dmle;
        char temp[2000];


	/*
	 * Your code goes here
	 */
    if (!memberNode->inited || !memberNode->inGroup || memberNode->bFailed)
       return;
    
        #ifdef DEBUGLOG
              MemberListEntry entry;
              for (unsigned int i = 0; i < memberNode->memberList.size(); i++)
              {
                 entry = memberNode->memberList.at(i);
                 sprintf(temp,"LoopOps entry %d id:%d heartbeat:%u timestamp:%u", i, entry.getid(), entry.getheartbeat(), entry.gettimestamp());
                 log->LOG(&memberNode->addr, temp);
               }
        #endif
    // go through MemberEntryList to check any node becoming fauly or not by comparing timeout and last heartbeat
    // if heartbeat is not updated for timeout period put it in suspected list

             for (unsigned int i = 0; i < memberNode->memberList.size(); i++)
             {
                dmle = memberNode->memberList.at(i);
                if ((dmle.getid() !=-1) && ((par->getcurrtime() - dmle.gettimestamp()) > TFAIL)) {
                   cout << i << " **" << dmle.getid() << " suspecting node failure curtime " << par->getcurrtime() << " timestamp:" << dmle.gettimestamp() << endl;
                   sprintf(temp, "** %d suspecting node failure currtime %u while timestamp %u ", dmle.getid(), par->getcurrtime(), dmle.gettimestamp()); 
                 log->LOG(&memberNode->addr, temp);
                }
                
             }

    // Monitor timeout of suspected members, is suspected timeout criteria meets mark it as Fail 

    // First update own timestamp 
    // Update own heartbeat
    // pick two neighbours randomly and send Gossip style hearbeat message

    MemberListEntry mle;
    
    memberNode->heartbeat = memberNode->heartbeat + 1;

    mle = memberNode->memberList.at(nid);
    mle.setheartbeat(memberNode->heartbeat);
    mle.settimestamp(par->getcurrtime());
    memberNode->memberList.at(nid) = mle;
    mmle = memberNode->memberList.data();


#if 0
             //  DEBUG update memberListEntry from recived ones
             for (unsigned int i = 0; i < memberNode->memberList.size(); i++)
             {
                dmle = memberNode->memberList.at(i);
                cout << i << ":: before sent" << dmle.getid() << " heartbeat:" << dmle.getheartbeat() << " timestamp:" << dmle.gettimestamp() << endl;
                
             }
              

    std::cout << "[" << par->getcurrtime() << "]" << "from node:" << nid << " to node:" << gossipIndex << " sending HEARTBEAT... " << endl;
#endif
    int lastIndex;
    for (int i = 0; i < 2; i++) {
       do 
       {
          gossipIndex = rand() % (par->MAX_NNB+1);
          mle =  memberNode->memberList.at(gossipIndex);
       
       } while ((gossipIndex==0) || (gossipIndex == nid) || (mle.getid()==-1) || ( i > 0 && gossipIndex == lastIndex));

       lastIndex = gossipIndex;
       
       Address to = getAddressFromId(gossipIndex);

       sendMsgHeartbeat(&memberNode->addr, &to, HEARTBEAT, memberNode->heartbeat, 100, &mmle,memberNode->memberList.size());
    }
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

Address MP1Node::getAddressFromId(int id) {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = id;
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
           MemberListEntry* nle = new MemberListEntry(-1,-1,0,0);
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
