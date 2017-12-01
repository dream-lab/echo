package in.dream_lab.nifi.tf.processors;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@TriggerWhenEmpty
@Tags({"local", "files", "filesystem", "ingest", "ingress", "get", "source", "input", "restricted"})
@CapabilityDescription("Creates FlowFiles from files in a directory.  NiFi will ignore files it doesn't have at least read permissions for.")
@WritesAttributes({
    @WritesAttribute(attribute = "filename", description = "The filename is set to the name of the file on disk"),
    @WritesAttribute(attribute = "path", description = "The path is set to the relative path of the file's directory on disk. For example, "
            + "if the <Input Directory> property is set to /tmp, files picked up from /tmp will have the path attribute set to ./. If "
            + "the <Recurse Subdirectories> property is set to true and a file is picked up from /tmp/abc/1/2/3, then the path attribute will "
            + "be set to abc/1/2/3"),
    @WritesAttribute(attribute = "file.creationTime", description = "The date and time that the file was created. May not work on all file systems"),
    @WritesAttribute(attribute = "file.lastModifiedTime", description = "The date and time that the file was last modified. May not work on all "
            + "file systems"),
    @WritesAttribute(attribute = "file.lastAccessTime", description = "The date and time that the file was last accessed. May not work on all "
            + "file systems"),
    @WritesAttribute(attribute = "file.owner", description = "The owner of the file. May not work on all file systems"),
    @WritesAttribute(attribute = "file.group", description = "The group owner of the file. May not work on all file systems"),
    @WritesAttribute(attribute = "file.permissions", description = "The read/write/execute permissions of the file. May not work on all file systems"),
    @WritesAttribute(attribute = "absolute.path", description = "The full/absolute path from where a file was picked up. The current 'path' "
            + "attribute is still populated, but may be a relative path")})
@Restricted("Provides operator the ability to read from and delete any file that NiFi has access to.")
public class GetSmbFiles extends AbstractProcessor {

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Input Directory")
            .description("The input directory from which to pull files")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
   
    public static final PropertyDescriptor KEEP_SOURCE_FILE = new PropertyDescriptor.Builder()
            .name("Keep Source File")
            .description("If true, the file is not deleted after it has been copied to the Content Repository; "
                    + "this causes the file to be picked up continually and is useful for testing purposes.  "
                    + "If not keeping original NiFi will need write permissions on the directory it is pulling "
                    + "from otherwise it will ignore the file.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor FILE_FILTER = new PropertyDescriptor.Builder()
            .name("File Filter")
            .description("Only files whose names match the given regular expression will be picked up")
            .required(true)
            .defaultValue("[^\\.].*")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();
  
    
    public static final PropertyDescriptor POLLING_INTERVAL = new PropertyDescriptor.Builder()
            .name("Polling Interval")
            .description("Indicates how long to wait before performing a directory listing")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("0 sec")
            .build();
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The maximum number of files to pull in each iteration")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();

    public static final String FILE_CREATION_TIME_ATTRIBUTE = "file.creationTime";
    public static final String FILE_LAST_MODIFY_TIME_ATTRIBUTE = "file.lastModifiedTime";
    public static final String FILE_LAST_ACCESS_TIME_ATTRIBUTE = "file.lastAccessTime";
    public static final String FILE_OWNER_ATTRIBUTE = "file.owner";
    public static final String FILE_GROUP_ATTRIBUTE = "file.group";
    public static final String FILE_PERMISSIONS_ATTRIBUTE = "file.permissions";
    public static final String FILE_MODIFY_DATE_ATTR_FORMAT = "yyyy-MM-dd'T'HH:mm:ssZ";

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("All files are routed to success").build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    private final BlockingQueue<SmbFile> fileQueue = new LinkedBlockingQueue<SmbFile>();
    private final Set<SmbFile> inProcess = new HashSet<SmbFile>();    // guarded by queueLock
    private final Set<SmbFile> recentlyProcessed = new HashSet<SmbFile>();    // guarded by queueLock
    private final Lock queueLock = new ReentrantLock();

    private final Lock listingLock = new ReentrantLock();

    private final AtomicLong queueLastUpdated = new AtomicLong(0L);
	private NtlmPasswordAuthentication auth;
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<PropertyDescriptor>();
        properties.add(DIRECTORY);
        properties.add(FILE_FILTER);
        properties.add(BATCH_SIZE);
        properties.add(KEEP_SOURCE_FILE);
        properties.add(POLLING_INTERVAL);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
        String user = "ubuntu";
        String pass ="ubuntu";
        auth = new NtlmPasswordAuthentication("",user, pass);
     
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        fileQueue.clear();
    }

   
    private Set<SmbFile> performListing(final SmbFile directory2, final boolean recurseSubdirectories) throws SmbException {
        if (!directory2.canRead() || !directory2.canWrite()) {
            throw new IllegalStateException("Directory '" + directory2 + "' does not have sufficient permissions (i.e., not writable and readable)");
        }
        final Set<SmbFile> queue = new HashSet<SmbFile>();
        if (!directory2.exists()) {
            return queue;
        }

        final SmbFile[] children = directory2.listFiles();
        if (children == null) {
            return queue;
        }

        for (final SmbFile child : children) {
            if (child.isDirectory()) {
                if (recurseSubdirectories) {
                    queue.addAll(performListing(child,  recurseSubdirectories));
                }
            } else  {
                queue.add(child);
            }
        }

        return queue;
    }

    protected Map<String, String> getAttributesFromFile(final Path file) {
        Map<String, String> attributes = new HashMap<String, String>();
        try {
            FileStore store = Files.getFileStore(file);
            if (store.supportsFileAttributeView("basic")) {
                try {
                    final DateFormat formatter = new SimpleDateFormat(FILE_MODIFY_DATE_ATTR_FORMAT, Locale.US);
                    BasicFileAttributeView view = Files.getFileAttributeView(file, BasicFileAttributeView.class);
                    BasicFileAttributes attrs = view.readAttributes();
                    attributes.put(FILE_LAST_MODIFY_TIME_ATTRIBUTE, formatter.format(new Date(attrs.lastModifiedTime().toMillis())));
                    attributes.put(FILE_CREATION_TIME_ATTRIBUTE, formatter.format(new Date(attrs.creationTime().toMillis())));
                    attributes.put(FILE_LAST_ACCESS_TIME_ATTRIBUTE, formatter.format(new Date(attrs.lastAccessTime().toMillis())));
                } catch (Exception ignore) {
                } // allow other attributes if these fail
            }
            if (store.supportsFileAttributeView("owner")) {
                try {
                    FileOwnerAttributeView view = Files.getFileAttributeView(file, FileOwnerAttributeView.class);
                    attributes.put(FILE_OWNER_ATTRIBUTE, view.getOwner().getName());
                } catch (Exception ignore) {
                } // allow other attributes if these fail
            }
            if (store.supportsFileAttributeView("posix")) {
                try {
                    PosixFileAttributeView view = Files.getFileAttributeView(file, PosixFileAttributeView.class);
                    attributes.put(FILE_PERMISSIONS_ATTRIBUTE, PosixFilePermissions.toString(view.readAttributes().permissions()));
                    attributes.put(FILE_GROUP_ATTRIBUTE, view.readAttributes().group().getName());
                } catch (Exception ignore) {
                } // allow other attributes if these fail
            }
        } catch (IOException ioe) {
            // well then this FlowFile gets none of these attributes
        }

        return attributes;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session)  {
    	SmbFile directory = null;
	   
    	FlowFile ff = session.get();
	    String batchId = ff.getAttribute("batchID");
	    session.remove(ff);
        final boolean keepingSourceFile = context.getProperty(KEEP_SOURCE_FILE).asBoolean();
        final ComponentLog logger = getLogger();

        if (fileQueue.size() < 100) {
            final long pollingMillis = context.getProperty(POLLING_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);
            if ((queueLastUpdated.get() < System.currentTimeMillis() - pollingMillis) && listingLock.tryLock() ) {
                try {
                	directory = new SmbFile(context.getProperty(DIRECTORY).evaluateAttributeExpressions().getValue()+"/"+batchId,auth);
                    final Set<SmbFile> listing = performListing(directory, false);

                    queueLock.lock();
                    try {
                        listing.removeAll(inProcess);
                        if (!keepingSourceFile) {
                            listing.removeAll(recentlyProcessed);
                        }

                        fileQueue.clear();
                        fileQueue.addAll(listing);

                        queueLastUpdated.set(System.currentTimeMillis());
                        recentlyProcessed.clear();

                        if (listing.isEmpty()) {
                            context.yield();
                        }
                    } finally {
                        queueLock.unlock();
                    }
                } catch (MalformedURLException | ProcessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SmbException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
                    listingLock.unlock();
                }
            }
        }

        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final List<SmbFile> files = new ArrayList<SmbFile>(batchSize);
        queueLock.lock();
        try {
            fileQueue.drainTo(files, batchSize);
            if (files.isEmpty()) {
                return;
            } else {
                inProcess.addAll(files);
            }
        } finally {
            queueLock.unlock();
        }

        final ListIterator<SmbFile> itr = files.listIterator();
        FlowFile flowFile = null;
        try {
            final Path directoryPath = Paths.get(directory.getPath());
            while (itr.hasNext()) {
                final SmbFile file = itr.next();
                final Path filePath = Paths.get(file.getPath().replaceAll(":/", "://"));
                System.out.println("File: "+filePath.toString());
                final Path relativePath = directoryPath.relativize(filePath.getParent());
                System.out.println("Rel: "+relativePath.toString());
                String relativePathString = relativePath.toString() + "/";
                if (relativePathString.isEmpty()) {
                    relativePathString = "./";
                }
                final Path absPath = filePath.toAbsolutePath();
                final String absPathString = absPath.getParent().toString() + "/";

                flowFile = session.create();
                final long importStart = System.nanoTime();
                flowFile = session.importFrom(file.getInputStream(), flowFile);
                final long importNanos = System.nanoTime() - importStart;
                final long importMillis = TimeUnit.MILLISECONDS.convert(importNanos, TimeUnit.NANOSECONDS);

                flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), file.getName());
                //flowFile = session.putAttribute(flowFile, CoreAttributes.PATH.key(), relativePathString);
                //flowFile = session.putAttribute(flowFile, CoreAttributes.ABSOLUTE_PATH.key(), absPathString);
               
                
				

                session.getProvenanceReporter().receive(flowFile, file.toURL().toString(), importMillis);
                session.transfer(flowFile, REL_SUCCESS);
                logger.info("added {} to flow", new Object[]{flowFile});

                if (!isScheduled()) {  // if processor stopped, put the rest of the files back on the queue.
                    queueLock.lock();
                    try {
                        while (itr.hasNext()) {
                            final SmbFile nextFile = itr.next();
                            fileQueue.add(nextFile);
                            inProcess.remove(nextFile);
                        }
                    } finally {
                        queueLock.unlock();
                    }
                }
            }
            session.commit();
        } catch (final Exception e) {
            logger.error("Failed to retrieve files due to {}", e);

            // anything that we've not already processed needs to be put back on the queue
            if (flowFile != null) {
                session.remove(flowFile);
            }
        } finally {
            queueLock.lock();
            try {
                inProcess.removeAll(files);
                recentlyProcessed.addAll(files);
            } finally {
                queueLock.unlock();
            }
        }
    }

}
