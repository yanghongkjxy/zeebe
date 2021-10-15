package io.camunda.zeebe.snapshots.impl;

import static io.camunda.zeebe.snapshots.impl.SnapshotChecksumTest.createChunk;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SfvChecksumTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private SfvChecksum sfvChecksum;

  @Before
  public void setUp() throws Exception {
    sfvChecksum = new SfvChecksum();
  }

  @Test
  public void shouldUseDefaultValueZeroWhenInitialized() {
    assertThat(sfvChecksum.getCombinedValue()).isEqualTo(0);
  }

  @Test
  public void shouldReadCombinedValueFromComment() {
    String[] sfvLines = {"; a simple comment to be ignored", "; combinedValue = bbaaccdd"};

    sfvChecksum.updateFromSfvFile(sfvLines);

    assertThat(sfvChecksum.getCombinedValue()).isEqualTo(0xbbaaccddL);
  }

  @Test
  public void shouldReadAndWriteSameValues() throws IOException {
    String[] givenSfvLines = {"; combinedValue = 12345678", "file1   aabbccdd"};

    // setup
    sfvChecksum.updateFromSfvFile(givenSfvLines);
    String serialized = new String(sfvChecksum.serializeSfvFileData(), StandardCharsets.UTF_8);

    // when
    String[] actualSfVlines = serialized.split(System.lineSeparator());

    // then
    assertThat(actualSfVlines).contains(givenSfvLines[0]);
    assertThat(actualSfVlines).contains(givenSfvLines[1]);
  }

  @Test
  public void shouldWriteSnapshotDirectoryCommentIfPresent() throws IOException {
    sfvChecksum.setSnapshotDirectoryComment("/foo/bar");

    String serialized = new String(sfvChecksum.serializeSfvFileData(), StandardCharsets.UTF_8);

    assertThat(serialized).contains("; snapshot directory = /foo/bar");
  }

  @Test
  public void shouldContainHumanReadableInstructions() throws IOException {
    String serialized = new String(sfvChecksum.serializeSfvFileData(), StandardCharsets.UTF_8);

    assertThat(serialized)
        .contains("; This is an SFC checksum file for all files in the given directory.");
    assertThat(serialized)
        .contains("; You might use cksfv or another tool to validate these files manually.");
    assertThat(serialized)
        .contains("; This is an automatically created file - please do NOT modify.");
  }

  @Test
  public void shouldThrowExceptionWhenUsingPreDefinedChecksumFromSfv() throws IOException {
    // setup
    var folder = temporaryFolder.newFolder().toPath();
    createChunk(folder, "file1.txt");

    // given
    sfvChecksum.updateFromSfvFile("; combinedValue = 12341234");

    // when
    assertThatThrownBy(() -> sfvChecksum.updateFromFile(folder))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
