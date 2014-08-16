package com.example;

public class ProcessingEvent {

    private long itemId;
    private long groupId;

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public long getItemId() {
        return itemId;
    }

    public void setGroupId(long groupId) {
        this.groupId = groupId;
    }

    public long getGroupId() {
        return groupId;
    }

    @Override
    public String toString() {
        return String.format("event (id %d, group %d)", itemId, groupId);
    }
}
