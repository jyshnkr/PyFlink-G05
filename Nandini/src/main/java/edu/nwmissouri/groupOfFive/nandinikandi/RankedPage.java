package edu.nwmissouri.groupOfFive.nandinikandi;

import java.util.ArrayList;

public class RankedPage {

    public String key;
    public ArrayList<VotingPage> voters;

    public RankedPage(String key, ArrayList<VotingPage> voters) {
        this.key = key;
        this.voters = voters;
    }

    public String getKey(){
        return key;
    }

    public  ArrayList<VotingPage> getVoters(){
        return voters;
    }

    public void setKey(String key){
        this.key = key;
    }

    public  void setVoters(ArrayList<VotingPage> voters){
        this.voters = voters;
    }

}
