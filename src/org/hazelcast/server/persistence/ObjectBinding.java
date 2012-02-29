package org.hazelcast.server.persistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.jboss.serial.io.JBossObjectInputStream;
import org.jboss.serial.io.JBossObjectOutputStream;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.DatabaseEntry;

public class ObjectBinding<E> implements EntryBinding<E> {
  /**
   * Creates a byte Object binding.
   */
  public ObjectBinding() {
  }

  private byte[] serialize(Object obj) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    JBossObjectOutputStream oos = new JBossObjectOutputStream(baos);
    oos.writeObject(obj);
    return baos.toByteArray();
  }

  private Object deserialize(byte[] bb) throws IOException, ClassNotFoundException {
    ByteArrayInputStream bais = new ByteArrayInputStream(bb);
    JBossObjectInputStream ois = new JBossObjectInputStream(bais);
    return ois.readObject();

  }

  @SuppressWarnings("unchecked")
  @Override
  public E entryToObject(DatabaseEntry entry) {
    int len = entry.getSize();
    if (len == 0) {
      return null;
    } else {
      try {
        return (E) deserialize(entry.getData());
      } catch (Exception e) {
        e.printStackTrace();
        return null;
      }
    }
  }

  @Override
  public void objectToEntry(E object, DatabaseEntry entry) {
    try {
      entry.setData(serialize(object));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}