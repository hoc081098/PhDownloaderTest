//
//  PhDownloadTask.swift
//  PhDownloader Example
//
//  Created by Petrus on 7/15/20.
//  Copyright © 2020 Petrus Nguyễn Thái Học. All rights reserved.
//

import Foundation

/// A model class encapsulates all information about download task
public struct PhDownloadTask: Hashable {
  /// The request that executes this task
  let request: PhDownloadRequest

  /// The latest state of download task
  let state: PhDownloadState

  public init(request: PhDownloadRequest, state: PhDownloadState) {
    self.request = request
    self.state = state
  }
}
